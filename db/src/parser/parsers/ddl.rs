use super::ParserError;
use crate::{
    Type, Value,
    parser::{
        Parser,
        statements::{
            AlterAction, AlterTableStatement, ColumnDef, CreateIndexStatement,
            CreateTableStatement, DropIndexStatement, DropStatement, ShowIndexesStatement,
            Statement,
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
        let table_name = self.expect_ident()?;
        Ok(DropStatement {
            table_name,
            if_exists,
        })
    }

    /// Parses an `ALTER TABLE` statement.
    ///
    /// Expects the token sequence `ALTER TABLE [IF EXISTS] <table_name> <action>`.
    ///
    /// Parses:
    /// - the `ALTER TABLE` keywords,
    /// - an optional `IF EXISTS`,
    /// - the target table name,
    /// - and an alter action (add, drop, or rename column/table).
    ///
    /// # Returns
    ///
    /// An [`AlterTableStatement`] containing the table name, `if_exists` flag, and the action.
    ///
    /// # Errors
    ///
    /// Returns a [`ParserError`] if any part of the statement is malformed or an expected token is
    /// missing.
    pub(super) fn parse_alter_table(&mut self) -> Result<AlterTableStatement, ParserError> {
        self.expect_seq(&[TokenType::Alter, TokenType::Table])?;
        let if_exists = self.parse_if_exists(false)?;

        let table_name = self.expect_ident()?;
        let action = self.parse_alter_action()?;
        Ok(AlterTableStatement {
            table_name,
            if_exists,
            action,
        })
    }

    /// Parses the action clause for an `ALTER TABLE` statement.
    ///
    /// Supported actions are:
    /// - `ADD COLUMN <column_definition>` — Add a new column to the table.
    /// - `DROP COLUMN [IF EXISTS] <name>` — Remove a column by name, optionally only if it exists.
    /// - `RENAME COLUMN <from> TO <to>` — Rename a column.
    /// - `RENAME TABLE TO <to>` — Rename the whole table.
    ///
    /// # Returns
    ///
    /// An [`AlterAction`] describing the type of alteration.
    ///
    /// # Errors
    ///
    /// Returns a [`ParserError`] if the clause is syntactically incorrect or an expected token is
    /// missing.
    fn parse_alter_action(&mut self) -> Result<AlterAction, ParserError> {
        let action_token = self.bump()?;
        match action_token.kind {
            TokenType::Add => {
                // ADD COLUMN <name> <type and constraints>
                self.expect_seq(&[TokenType::Column])?;
                let name = self.expect_ident()?;
                let column_def = self.parse_column_definition(name)?;
                Ok(AlterAction::AddColumn(column_def))
            }

            TokenType::Drop => {
                // DROP COLUMN [IF EXISTS] <name>
                self.expect_seq(&[TokenType::Column])?;
                let if_exists = self.parse_if_exists(false)?;
                let name = self.expect_ident()?;
                Ok(AlterAction::DropColumn { name, if_exists })
            }

            TokenType::Rename => {
                // RENAME COLUMN <from> TO <to> | RENAME TO <to>
                if self.if_peek_then_consume(TokenType::Column)? {
                    let from = self.expect_ident()?;
                    self.expect(TokenType::To)?;
                    let to = self.expect_ident()?;
                    Ok(AlterAction::RenameColumn { from, to })
                } else {
                    self.expect(TokenType::To)?;
                    let to = self.expect_ident()?;
                    Ok(AlterAction::RenameTable { to })
                }
            }

            _ => Err(ParserError::ParsingError(format!(
                "expected ADD COLUMN, DROP COLUMN, RENAME COLUMN, or RENAME TABLE, got {}",
                action_token.value
            ))),
        }
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
    pub(super) fn parse_create(&mut self) -> Result<CreateTableStatement, ParserError> {
        self.expect_seq(&[TokenType::Table])?;
        let if_not_exists = self.parse_if_exists(true)?;
        let table_name = self.expect_ident()?;
        self.expect(TokenType::Lparen)?;

        let mut columns = Vec::new();
        let mut primary_key = Vec::new();

        loop {
            let curr_tok = self.bump()?;
            match curr_tok.kind {
                TokenType::Primary => {
                    self.expect_seq(&[TokenType::Key])?;
                    primary_key.extend(self.paren_list(Parser::expect_ident)?);
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

        Ok(CreateTableStatement {
            table_name,
            if_not_exists,
            columns,
            primary_key,
        })
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

    /// Parses `CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<col>, ...) [USING HASH|BTREE]`.
    ///
    /// Defaults to [`Index::Hash`] when no `USING` clause is present. At least
    /// one column is required; column order is preserved (semantically
    /// significant for composite B-tree indexes via the leftmost-prefix rule).
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if required tokens are missing, the column list
    /// is empty, or the index type after `USING` is not `HASH` or `BTREE`.
    pub(super) fn parse_create_index(&mut self) -> Result<CreateIndexStatement, ParserError> {
        self.expect(TokenType::Index)?;
        let if_not_exists = self.parse_if_exists(true)?;
        let index_name = self.expect_ident()?;

        self.expect(TokenType::On)?;
        let table_name = self.expect_ident()?;

        let columns = self.paren_list(Parser::expect_ident)?;
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

        Ok(CreateIndexStatement {
            index_name,
            table_name,
            columns,
            index_type,
            if_not_exists,
        })
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

        let index_name = self.expect_ident()?;
        let table_name = if self.peek_is(TokenType::On)? {
            self.bump()?;
            self.expect_ident()?
        } else {
            String::new()
        };

        Ok(DropIndexStatement {
            table_name,
            index_name,
            if_exists,
        })
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
                Ok(Some(self.expect_ident()?))
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
        assert!(t.primary_key.is_empty());
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
        assert_eq!(t.primary_key, vec!["id"]);
    }

    #[test]
    fn test_parse_create_primary_key_clause_before_columns() {
        let stmt = parse("CREATE TABLE t (PRIMARY KEY (id), id INT)").unwrap();
        let Statement::CreateTable(t) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(t.primary_key, vec!["id"]);
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

    #[test]
    fn test_parse_create_index_defaults_to_hash() {
        let stmt = parse("CREATE INDEX idx_email ON users (email)").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert_eq!(c.index_name, "idx_email");
        assert_eq!(c.table_name, "users");
        assert_eq!(c.columns, vec!["email"]);
        let rendered = c.to_string();
        assert!(rendered.contains("idx_email"));
        assert!(rendered.contains("users"));
        assert!(rendered.contains("email"));
        assert!(rendered.contains("USING HASH"));
    }

    #[test]
    fn test_parse_create_index_multiple_columns_preserve_order() {
        let stmt = parse("CREATE INDEX idx_name ON users (last_name, first_name, email)").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert_eq!(c.columns, vec!["last_name", "first_name", "email"]);
        let rendered = c.to_string();
        assert!(rendered.contains("(last_name, first_name, email)"));
    }

    #[test]
    fn test_parse_create_index_if_not_exists() {
        let stmt = parse("CREATE INDEX IF NOT EXISTS ix ON t (a, b)").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert!(c.if_not_exists);
        assert_eq!(c.columns, vec!["a", "b"]);
        assert!(c.to_string().contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_parse_create_index_using_hash() {
        let stmt = parse("CREATE INDEX h ON t (c) USING HASH").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert!(c.to_string().contains("USING HASH"));
    }

    #[test]
    fn test_parse_create_index_using_btree_multi_column() {
        let stmt = parse("CREATE INDEX b ON t (a, b) USING BTREE").unwrap();
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert_eq!(c.columns, vec!["a", "b"]);
        assert!(c.to_string().contains("USING BTREE"));
    }

    #[test]
    fn test_parse_create_index_empty_column_list_rejected() {
        // paren_list requires at least one identifier, so `()` fails on the closing `)`.
        let Err(err) = parse("CREATE INDEX ix ON t ()") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Identifier,
            found: TokenType::Rparen,
        }));
    }

    #[test]
    fn test_parse_create_index_trailing_comma_rejected() {
        assert!(parse("CREATE INDEX ix ON t (a, )").is_err());
    }

    #[test]
    fn test_parse_create_index_using_invalid_kind() {
        let Err(ParserError::ParsingError(msg)) = parse("CREATE INDEX bad ON t (c) USING HEAP")
        else {
            panic!("expected ParsingError");
        };
        assert!(msg.contains("expected HASH or BTREE"));
    }

    #[test]
    fn test_parse_create_index_missing_on() {
        // Without ON, parser should fail looking for `ON` after the index name.
        let Err(err) = parse("CREATE INDEX ix (c)") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::On,
            ..
        }));
    }

    #[test]
    fn test_parse_create_index_missing_table() {
        assert!(matches!(
            parse("CREATE INDEX ix ON"),
            Err(ParserError::WantedToken)
        ));
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

    use crate::parser::statements::AlterAction;

    #[test]
    fn test_parse_alter_add_column_simple() {
        let stmt = parse("ALTER TABLE users ADD COLUMN age INT").unwrap();
        let Statement::AlterTable(a) = stmt else {
            panic!("expected AlterTable");
        };
        assert_eq!(a.table_name, "users");
        assert!(!a.if_exists);
        let AlterAction::AddColumn(col) = a.action else {
            panic!("expected AddColumn");
        };
        assert_eq!(col.name, "age");
        assert_eq!(col.col_type, Type::Int64);
        assert!(col.nullable);
        assert!(!col.primary_key);
        assert!(col.default.is_none());
    }

    #[test]
    fn test_parse_alter_add_column_with_qualifiers() {
        let stmt = parse("ALTER TABLE users ADD COLUMN age INT NOT NULL DEFAULT 0").unwrap();
        let Statement::AlterTable(a) = stmt else {
            panic!("expected AlterTable");
        };
        let AlterAction::AddColumn(col) = a.action else {
            panic!("expected AddColumn");
        };
        assert_eq!(col.name, "age");
        assert!(!col.nullable);
        assert_eq!(col.default, Some(Value::Int64(0)));
    }

    #[test]
    fn test_parse_alter_drop_column() {
        let stmt = parse("ALTER TABLE users DROP COLUMN bio").unwrap();
        let Statement::AlterTable(a) = stmt else {
            panic!("expected AlterTable");
        };
        let AlterAction::DropColumn { name, if_exists } = a.action else {
            panic!("expected DropColumn");
        };
        assert_eq!(name, "bio");
        assert!(!if_exists);
    }

    #[test]
    fn test_parse_alter_drop_column_if_exists() {
        let stmt = parse("ALTER TABLE users DROP COLUMN IF EXISTS bio").unwrap();
        let Statement::AlterTable(a) = stmt else {
            panic!("expected AlterTable");
        };
        let AlterAction::DropColumn { name, if_exists } = a.action else {
            panic!("expected DropColumn");
        };
        assert_eq!(name, "bio");
        assert!(if_exists);
    }

    #[test]
    fn test_parse_alter_rename_column() {
        let stmt = parse("ALTER TABLE users RENAME COLUMN name TO full_name").unwrap();
        let Statement::AlterTable(a) = stmt else {
            panic!("expected AlterTable");
        };
        let AlterAction::RenameColumn { from, to } = a.action else {
            panic!("expected RenameColumn");
        };
        assert_eq!(from, "name");
        assert_eq!(to, "full_name");
    }

    #[test]
    fn test_parse_alter_rename_table() {
        let stmt = parse("ALTER TABLE users RENAME TO accounts").unwrap();
        let Statement::AlterTable(a) = stmt else {
            panic!("expected AlterTable");
        };
        assert_eq!(a.table_name, "users");
        let AlterAction::RenameTable { to } = a.action else {
            panic!("expected RenameTable");
        };
        assert_eq!(to, "accounts");
    }

    #[test]
    fn test_parse_alter_outer_if_exists() {
        let stmt = parse("ALTER TABLE IF EXISTS users RENAME TO accounts").unwrap();
        let Statement::AlterTable(a) = stmt else {
            panic!("expected AlterTable");
        };
        assert_eq!(a.table_name, "users");
        assert!(a.if_exists);
        assert!(matches!(a.action, AlterAction::RenameTable { .. }));
    }

    #[test]
    fn test_parse_alter_display_round_trip() {
        let stmt = parse("ALTER TABLE users ADD COLUMN age INT NOT NULL").unwrap();
        let rendered = stmt.to_string();
        assert!(rendered.starts_with("ALTER TABLE users"));
        assert!(rendered.contains("ADD COLUMN"));
        assert!(rendered.contains("age"));
        assert!(rendered.contains("NOT NULL"));
    }

    // --- error paths: parse_alter_table ---

    #[test]
    fn test_parse_alter_add_missing_column_keyword() {
        let Err(err) = parse("ALTER TABLE users ADD age INT") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Column,
            ..
        }));
    }

    #[test]
    fn test_parse_alter_drop_missing_column_keyword() {
        let Err(err) = parse("ALTER TABLE users DROP bio") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::Column,
            ..
        }));
    }

    #[test]
    fn test_parse_alter_rename_column_missing_to() {
        let Err(err) = parse("ALTER TABLE users RENAME COLUMN name full_name") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::To,
            ..
        }));
    }

    #[test]
    fn test_parse_alter_rename_table_missing_to() {
        let Err(err) = parse("ALTER TABLE users RENAME accounts") else {
            panic!("expected error");
        };
        assert!(matches!(err, ParserError::UnexpectedToken {
            expected: TokenType::To,
            ..
        }));
    }

    #[test]
    fn test_parse_alter_unknown_action() {
        let Err(ParserError::ParsingError(msg)) = parse("ALTER TABLE users FLIP COLUMN x") else {
            panic!("expected ParsingError");
        };
        assert!(msg.contains("ADD") || msg.contains("expected"));
    }

    #[test]
    fn test_parse_alter_missing_action() {
        assert!(matches!(
            parse("ALTER TABLE users"),
            Err(ParserError::WantedToken)
        ));
    }

    #[test]
    fn test_parse_alter_missing_table_name() {
        assert!(matches!(
            parse("ALTER TABLE"),
            Err(ParserError::WantedToken)
        ));
    }
}
