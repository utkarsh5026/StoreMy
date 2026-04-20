use super::ParserError;
use crate::{
    Type, Value,
    parser::{
        Parser,
        statements::{
            ColumnDef, CreateIndexStatement, DropIndexStatement, DropStatement,
            ShowIndexesStatement, Statement,
        },
        token::TokenType,
    },
    storage::index::Index,
};

impl Parser {
    /// Parses `DROP TABLE [IF EXISTS] <name>`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the keyword sequence or table name is missing.
    pub(super) fn parse_drop(&mut self) -> Result<DropStatement, ParserError> {
        self.expect_seq(&[TokenType::Table])?;
        let if_exists = self.parse_if_exists(false)?;
        let tok = self.expect(TokenType::Identifier)?;
        Ok(Statement::drop(&tok, if_exists))
    }

    /// Parses `CREATE TABLE [IF NOT EXISTS] <name> (<columns>)`.
    ///
    /// Handles column definitions (type, nullability, `PRIMARY KEY`,
    /// `AUTO_INCREMENT`, `DEFAULT`) as well as a trailing `PRIMARY KEY (<col>)`
    /// clause.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the syntax is malformed, a type token is
    /// unrecognized, or a default value can't be parsed.
    pub(super) fn parse_create(&mut self) -> Result<Statement, ParserError> {
        self.expect_seq(&[TokenType::Table])?;
        let if_not_exists = self.parse_if_exists(true)?;
        let table_name = self.expect(TokenType::Identifier)?.value;
        self.expect(TokenType::Lparen)?;

        let mut columns: Vec<ColumnDef> = Vec::new();
        let mut primary_key: Option<String> = None;

        loop {
            let curr_tok = self.bump()?;
            match curr_tok.kind {
                TokenType::Primary => {
                    self.expect_seq(&[TokenType::Key, TokenType::Lparen])?;
                    let pkey = self.expect(TokenType::Identifier)?;
                    self.expect(TokenType::Rparen)?;
                    primary_key = Some(pkey.value);
                }
                TokenType::Identifier => {
                    columns.push(self.parse_column_definition(curr_tok.value)?);
                }
                _ => {
                    return Err(ParserError::ParsingError(format!(
                        "expected column definition or PRIMARY KEY, got {}",
                        curr_tok.value
                    )));
                }
            }

            match self.bump()?.kind {
                TokenType::Rparen => break,
                TokenType::Comma => {}
                _ => return Err(ParserError::ParsingError("expected , or )".to_owned())),
            }
        }

        Ok(Statement::CreateTable(Statement::create_table(
            table_name,
            if_not_exists,
            columns,
            primary_key,
        )))
    }

    /// Parses the tail of a `CREATE TABLE` column: a [`Type`] token followed by
    /// zero or more optional column qualifiers, in any order, until the next
    /// token is not one of `NOT`, `PRIMARY`, `AUTO_INCREMENT`, or `DEFAULT`.
    ///
    /// Recognized qualifiers:
    /// - `NOT NULL` — column is not nullable (default is nullable).
    /// - `PRIMARY KEY` — marks this column as the row's primary key in the column list (separate
    ///   from a trailing table-level `PRIMARY KEY (...)`).
    /// - `AUTO_INCREMENT`
    /// - `DEFAULT` and a literal token — must convert via [`Value::try_from`].
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the type token is not a valid [`Type`], a
    /// qualifier sequence is malformed, the default literal is invalid, or the
    /// lexer fails while peeking ahead.
    fn parse_column_definition(&mut self, col_name: String) -> Result<ColumnDef, ParserError> {
        let type_tok = self.bump()?;
        let col_type = Type::try_from(type_tok).map_err(ParserError::ParsingError)?;

        let mut nullable = true;
        let mut is_primary_key = false;
        let mut auto_increment = false;
        let mut default: Option<Value> = None;

        loop {
            let next_kind = match self.lexer.next() {
                None => break,
                Some(Err(e)) => return Err(ParserError::from(e)),
                Some(Ok(tok)) => {
                    let k = tok.kind;
                    self.lexer.backtrack().map_err(ParserError::from)?;
                    k
                }
            };

            match next_kind {
                TokenType::Not => {
                    self.expect_seq(&[TokenType::Not, TokenType::Null])?;
                    nullable = false;
                }
                TokenType::Primary => {
                    self.expect_seq(&[TokenType::Primary, TokenType::Key])?;
                    is_primary_key = true;
                }
                TokenType::AutoIncrement => {
                    self.bump()?;
                    auto_increment = true;
                }
                TokenType::Default => {
                    self.bump()?;
                    let val_tok = self.bump()?;
                    default = Some(Value::try_from(val_tok).map_err(ParserError::ParsingError)?);
                }
                _ => break,
            }
        }

        Ok(ColumnDef {
            name: col_name,
            col_type,
            nullable,
            primary_key: is_primary_key,
            auto_increment,
            default,
        })
    }

    /// Tries to consume an optional `IF [NOT] EXISTS` clause.
    ///
    /// When `with_not` is `true` the expected form is `IF NOT EXISTS`
    /// (used for `CREATE`).  When `false` the expected form is `IF EXISTS`
    /// (used for `DROP`).  Returns `false` and backtracks if `IF` is absent.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if `IF` is present but the rest of the clause is
    /// malformed.
    fn parse_if_exists(&mut self, with_not: bool) -> Result<bool, ParserError> {
        let if_tok = self.bump()?;
        if if_tok.is_not(TokenType::If) {
            self.lexer.backtrack()?;
            return Ok(false);
        }

        if with_not {
            self.expect_seq(&[TokenType::Not, TokenType::Exists])?;
        } else {
            self.expect_seq(&[TokenType::Exists])?;
        }
        Ok(true)
    }

    /// Parses `CREATE INDEX [IF NOT EXISTS] <name> (<col>) [USING HASH|BTREE]`.
    ///
    /// Defaults to [`Index::Hash`] when no `USING` clause is present.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if required tokens are missing or the index type
    /// after `USING` is not `HASH` or `BTREE`.
    pub(super) fn parse_create_index(&mut self) -> Result<CreateIndexStatement, ParserError> {
        self.expect(TokenType::Index)?;
        let if_not_exists = self.parse_if_exists(true)?;

        let index_name = self.expect(TokenType::Identifier)?;

        self.expect(TokenType::Lparen)?;
        let col_name = self.expect(TokenType::Identifier)?;
        self.expect(TokenType::Rparen)?;

        let index_type = self
            .on_peek_token(TokenType::Using, |p| {
                let type_tok = p.bump()?;
                match type_tok.kind {
                    TokenType::Hash => Ok(Index::Hash),
                    TokenType::Btree => Ok(Index::Btree),
                    _ => Err(ParserError::ParsingError(format!(
                        "expected HASH or BTREE after USING, got {type_tok}"
                    ))),
                }
            })?
            .unwrap_or(Index::Hash);

        Ok(Statement::create_index(
            &index_name,
            &col_name,
            index_type,
            if_not_exists,
        ))
    }

    /// Parses `DROP INDEX [IF EXISTS] <name> [ON <table>]`.
    ///
    /// The `ON <table>` clause is optional; an empty string is stored when it
    /// is absent.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the keyword sequence or index name is missing.
    pub(super) fn parse_drop_index(&mut self) -> Result<DropIndexStatement, ParserError> {
        self.expect(TokenType::Index)?;
        let if_exists = self.parse_if_exists(false)?;

        let index_name = self.expect(TokenType::Identifier)?;
        let table_name = if self.peek_is(TokenType::On)? {
            self.bump()?;
            String::from(&self.expect(TokenType::Identifier)?)
        } else {
            String::new()
        };

        Ok(Statement::drop_index(table_name, &index_name, if_exists))
    }

    /// Parses `SHOW INDEXES [FROM <table>]`.
    ///
    /// The `FROM <table>` clause is optional; when absent the statement shows
    /// indexes for all tables.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the `SHOW INDEXES` keyword sequence is missing.
    pub(super) fn parse_show_index(&mut self) -> Result<ShowIndexesStatement, ParserError> {
        self.expect_seq(&[TokenType::Show, TokenType::Indexes])?;

        let table_name = self.peek_is(TokenType::From).and_then(|ok| {
            if ok {
                self.bump()?;
                let t = self.expect(TokenType::Identifier)?;
                Ok(Some(t.value))
            } else {
                Ok(None)
            }
        })?;

        Ok(Statement::show_indexes(table_name))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::{Parser, parsers::ParserError, statements::Statement, token::TokenType},
        types::{Type, Value},
    };

    fn parse(sql: &str) -> Result<Statement, ParserError> {
        Parser::new(sql).parse()
    }

    #[test]
    fn test_parse_drop_basic() {
        let stmt = parse("DROP TABLE users").unwrap();
        let Statement::Drop(d) = stmt else {
            panic!("expected Drop");
        };
        assert_eq!(d.table_name, "users");
        assert!(!d.if_exists);
    }

    #[test]
    fn test_parse_drop_if_exists() {
        let stmt = parse("DROP TABLE IF EXISTS orders").unwrap();
        let Statement::Drop(d) = stmt else {
            panic!("expected Drop");
        };
        assert_eq!(d.table_name, "orders");
        assert!(d.if_exists);
    }

    #[test]
    fn test_parse_drop_table_name_case_preserved() {
        let stmt = parse("DROP TABLE MixedCase").unwrap();
        let Statement::Drop(d) = stmt else {
            panic!("expected Drop");
        };
        assert_eq!(d.table_name, "MixedCase");
    }

    #[test]
    fn test_parse_drop_missing_table_name() {
        assert!(matches!(parse("DROP TABLE"), Err(ParserError::WantedToken)));
    }

    #[test]
    fn test_parse_drop_if_without_exists() {
        let Err(err) = parse("DROP TABLE IF users") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Exists,
            ..
        }));
    }

    #[test]
    fn test_parse_drop_if_not_exists_rejected() {
        let Err(err) = parse("DROP TABLE IF NOT EXISTS users") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Exists,
            found: TokenType::Not
        }));
    }

    #[test]
    fn test_parse_create_single_column() {
        let stmt = parse("CREATE TABLE items (id INT)").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.table_name, "items");
        assert!(!t.if_not_exists);
        assert_eq!(t.columns.len(), 1);
        assert_eq!(t.columns[0].name, "id");
        assert_eq!(t.columns[0].col_type, Type::Int64);
        assert!(t.columns[0].nullable);
        assert!(!t.columns[0].primary_key);
        assert!(!t.columns[0].auto_increment);
        assert!(t.columns[0].default.is_none());
        assert!(t.primary_key.is_none());
    }

    #[test]
    fn test_parse_create_if_not_exists() {
        let stmt = parse("CREATE TABLE IF NOT EXISTS meta (k STRING)").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert!(t.if_not_exists);
        assert_eq!(t.columns[0].col_type, Type::String);
    }

    #[test]
    fn test_parse_create_multiple_columns_and_trailing_primary_key() {
        let stmt = parse("CREATE TABLE users (id INT, name STRING, PRIMARY KEY (id))").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.primary_key.as_deref(), Some("id"));
    }

    #[test]
    fn test_parse_create_primary_key_clause_before_columns() {
        let stmt = parse("CREATE TABLE t (PRIMARY KEY (id), id INT)").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.primary_key.as_deref(), Some("id"));
        assert_eq!(t.columns.len(), 1);
        assert_eq!(t.columns[0].name, "id");
    }

    #[test]
    fn test_parse_create_column_modifiers() {
        let stmt = parse(concat!(
            "CREATE TABLE u (",
            "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, ",
            "bio TEXT, ",
            "score FLOAT DEFAULT 0, ",
            "alive BOOLEAN DEFAULT true",
            ")",
        ))
        .unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.columns.len(), 4);

        let id = &t.columns[0];
        assert!(!id.nullable);
        assert!(id.primary_key);
        assert!(id.auto_increment);
        assert!(id.default.is_none());

        assert_eq!(t.columns[1].col_type, Type::String);
        assert_eq!(t.columns[2].default, Some(Value::Int64(0)));
        assert_eq!(t.columns[3].default, Some(Value::Bool(true)));
    }

    #[test]
    fn test_parse_create_type_aliases_integer_and_text() {
        let stmt = parse("CREATE TABLE t (n INTEGER, body TEXT)").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.columns[0].col_type, Type::Int64);
        assert_eq!(t.columns[1].col_type, Type::String);
    }

    // --- edge cases: parse_create ---

    #[test]
    fn test_parse_create_default_string_literal() {
        let stmt = parse("CREATE TABLE t (msg STRING DEFAULT 'hi')").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.columns[0].default, Some(Value::String("hi".to_string())));
    }

    #[test]
    fn test_parse_create_default_null() {
        let stmt = parse("CREATE TABLE t (x INT DEFAULT NULL)").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.columns[0].default, Some(Value::Null));
    }

    // --- error paths: parse_create ---

    #[test]
    fn test_parse_create_empty_column_list() {
        let Err(ParserError::ParsingError(msg)) = parse("CREATE TABLE empty ()") else {
            panic!("expected ParsingError");
        };
        assert!(msg.contains("expected column definition"));
    }

    #[test]
    fn test_parse_create_unknown_column_type() {
        let Err(ParserError::ParsingError(msg)) = parse("CREATE TABLE bad (x UINT64)") else {
            panic!("expected ParsingError");
        };
        assert!(msg.contains("unknown data type"));
    }

    #[test]
    fn test_parse_create_bad_default_value_token() {
        let Err(ParserError::ParsingError(msg)) = parse("CREATE TABLE bad (x INT DEFAULT bogus)")
        else {
            panic!("expected ParsingError");
        };
        assert!(msg.contains("literal value"));
    }

    #[test]
    fn test_parse_create_missing_comma_between_columns() {
        let Err(ParserError::ParsingError(msg)) = parse("CREATE TABLE bad (a INT b INT)") else {
            panic!("expected ParsingError");
        };
        assert_eq!(msg, "expected , or )");
    }

    #[test]
    fn test_parse_create_if_exists_instead_of_if_not_exists() {
        let Err(err) = parse("CREATE TABLE IF EXISTS t (id INT)") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Not,
            found: TokenType::Exists
        }));
    }

    #[test]
    fn test_parse_create_if_not_incomplete() {
        let Err(err) = parse("CREATE TABLE IF NOT t (id INT)") else {
            panic!("expected error");
        };
        // After `IF NOT`, parser requires `EXISTS` before the table name.
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Exists,
            found: TokenType::Identifier,
        }));
    }

    // --- happy path: parse_create_index ---

    #[test]
    fn test_parse_create_index_defaults_to_hash() {
        let stmt = parse("CREATE INDEX idx_email (email)").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        let rendered = c.to_string();
        assert!(rendered.contains("idx_email"));
        assert!(rendered.contains("email"));
        assert!(rendered.contains("USING HASH"));
    }

    #[test]
    fn test_parse_create_index_if_not_exists() {
        let stmt = parse("CREATE INDEX IF NOT EXISTS ix (c)").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert!(c.to_string().contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_parse_create_index_using_hash() {
        let stmt = parse("CREATE INDEX h (c) USING HASH").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert!(c.to_string().contains("USING HASH"));
    }

    #[test]
    fn test_parse_create_index_using_btree() {
        let stmt = parse("CREATE INDEX b (c) USING BTREE").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert!(c.to_string().contains("USING BTREE"));
    }

    // --- error paths: parse_create_index ---

    #[test]
    fn test_parse_create_index_using_invalid_kind() {
        let Err(ParserError::ParsingError(msg)) = parse("CREATE INDEX bad (c) USING HEAP") else {
            panic!("expected ParsingError");
        };
        assert!(msg.contains("expected HASH or BTREE"));
    }

    #[test]
    fn test_parse_drop_index_basic_empty_on_table() {
        let stmt = parse("DROP INDEX idx1").unwrap();
        let Statement::DropIndex(d) = stmt else {
            panic!("expected DropIndex");
        };
        let dbg = format!("{d:?}");
        assert!(dbg.contains(r#""idx1""#), "index name: {dbg}");
        assert!(
            dbg.contains(r#"table_name: """#) || dbg.contains("table_name: \"\""),
            "{dbg}"
        );
        assert!(!dbg.contains("if_exists: true"));
    }

    #[test]
    fn test_parse_drop_index_if_exists_on_table() {
        let stmt = parse("DROP INDEX IF EXISTS ix ON tbl").unwrap();
        let Statement::DropIndex(d) = stmt else {
            panic!("expected DropIndex");
        };
        let dbg = format!("{d:?}");
        assert!(dbg.contains("if_exists: true"), "{dbg}");
        assert!(dbg.contains(r#""ix""#), "{dbg}");
        assert!(dbg.contains(r#""tbl""#), "{dbg}");
    }

    // --- error paths: parse_drop_index ---

    #[test]
    fn test_parse_drop_index_missing_name() {
        assert!(matches!(parse("DROP INDEX"), Err(ParserError::WantedToken)));
    }

    #[test]
    fn test_parse_drop_index_on_without_table() {
        assert!(matches!(
            parse("DROP INDEX ix ON"),
            Err(ParserError::WantedToken)
        ));
    }

    #[test]
    fn test_parse_drop_index_if_without_exists() {
        let Err(err) = parse("DROP INDEX IF ix") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Exists,
            ..
        }));
    }

    // --- happy path: parse_show_index ---

    #[test]
    fn test_parse_show_index_all_tables() {
        let stmt = parse("SHOW INDEXES").unwrap();
        let Statement::ShowIndexes(s) = stmt else {
            panic!("expected ShowIndexes");
        };
        assert!(s.0.is_none());
    }

    #[test]
    fn test_parse_show_index_from_table() {
        let stmt = parse("SHOW INDEXES FROM accounts").unwrap();
        let Statement::ShowIndexes(s) = stmt else {
            panic!("expected ShowIndexes");
        };
        assert_eq!(s.0.as_deref(), Some("accounts"));
    }

    // --- error paths: parse_show_index ---

    #[test]
    fn test_parse_show_index_from_without_identifier() {
        assert!(matches!(
            parse("SHOW INDEXES FROM"),
            Err(ParserError::WantedToken)
        ));
    }

    // --- property / invariant: Display round-trip shape ---

    #[test]
    fn test_parse_create_round_trip_display_contains_core_tokens() {
        let sql = "CREATE TABLE p (id INT NOT NULL PRIMARY KEY)";
        let stmt = parse(sql).unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        let out = t.to_string();
        assert!(out.starts_with("CREATE TABLE"));
        assert!(out.contains('p'));
        assert!(out.contains("id"));
        assert!(out.contains("INT"));
        assert!(out.contains("NOT NULL"));
        assert!(out.contains("PRIMARY KEY"));
    }
}
