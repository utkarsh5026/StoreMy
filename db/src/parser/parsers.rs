//! SQL statement parser.
//!
//! This module implements a recursive-descent parser that turns a stream of
//! [`Token`]s produced by [`crate::parser::lexer`] into typed [`Statement`]
//! AST nodes.  Each `parse_*` method handles one grammar production.
//!
//! The entry point is [`Parser::parse`], which dispatches to the appropriate
//! sub-parser based on the first keyword in the input.

use thiserror::Error;

use crate::{
    Type, Value,
    parser::{
        Parser,
        lexer::LexError,
        statements::{
            AggFunc, Assignment, ColumnDef, CreateIndexStatement, DeleteStatement,
            DropIndexStatement, DropStatement, InsertStatement, Join, JoinKind, Literal, OrderBy,
            OrderDirection, SelectColumns, SelectExpr, SelectStatement, ShowIndexesStatement,
            Statement, UpdateStatement, WhereCondition,
        },
        token::{Token, TokenType},
    },
    primitives::Predicate,
    storage::index::Index,
};

/// Errors that can occur while parsing a SQL statement.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    /// The parser required another token but the token stream was exhausted.
    #[error("Wanted Token")]
    WantedToken,

    /// The next token had the wrong kind.
    #[error("expected {expected}, but found {found:?}")]
    UnexpectedToken {
        /// The token kind the parser needed.
        expected: TokenType,
        /// The token kind that was actually present.
        found: TokenType,
    },

    /// A higher-level grammar rule was violated.
    ///
    /// The inner `String` describes what went wrong.
    #[error("parsing error from {0}")]
    ParsingError(String),

    /// A lexer error bubbled up during parsing.
    #[error(transparent)]
    LexError(#[from] LexError),
}

impl ParserError {
    /// Constructs an [`UnexpectedToken`](ParserError::UnexpectedToken) error.
    fn unexpected(expected: TokenType, found: TokenType) -> Self {
        Self::UnexpectedToken { expected, found }
    }
}

impl Parser {
    /// Parses a complete SQL statement from the token stream.
    ///
    /// Peeks at the first token to decide which sub-parser to call, then
    /// rewinds so the sub-parser can consume it normally.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the token stream is empty, starts with an
    /// unrecognized keyword, or any sub-parser fails.
    pub fn parse(&mut self) -> Result<Statement, ParserError> {
        let tok = self.ensure_next_token()?;
        self.lexer.backtrack()?;

        match tok.kind {
            TokenType::Delete => Ok(Statement::Delete(self.parse_delete()?)),
            TokenType::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            TokenType::Update => Ok(Statement::Update(self.parse_update()?)),
            TokenType::Show => Ok(Statement::ShowIndexes(self.parse_show_index()?)),
            TokenType::Select => Ok(Statement::Select(self.parse_select()?)),
            TokenType::Drop => {
                self.ensure_next_token()?; // consume DROP
                let next = self.ensure_next_token()?;
                self.lexer.backtrack()?; // put TABLE/INDEX back

                match next.kind {
                    TokenType::Index => Ok(Statement::DropIndex(self.parse_drop_index()?)),
                    TokenType::Table => Ok(Statement::Drop(self.parse_drop()?)),
                    _ => Err(ParserError::ParsingError(format!(
                        "expected TABLE or INDEX after DROP, got {}",
                        next.value
                    ))),
                }
            }
            TokenType::Create => {
                self.ensure_next_token()?;
                let next = self.ensure_next_token()?;
                self.lexer.backtrack()?;

                match next.kind {
                    TokenType::Index => Ok(Statement::CreateIndex(self.parse_create_index()?)),
                    TokenType::Table => self.parse_create(),
                    _ => Err(ParserError::ParsingError(format!(
                        "expected TABLE or INDEX after CREATE, got {}",
                        next.value
                    ))),
                }
            }
            _ => Err(ParserError::ParsingError(format!(
                "unexpected statement start: {}",
                tok.value
            ))),
        }
    }

    /// Consumes tokens one-by-one, checking that each matches the expected kind.
    ///
    /// Fails on the first mismatch. Useful for consuming fixed keyword sequences
    /// like `DROP TABLE` or `ORDER BY`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if any token doesn't match, or
    /// [`ParserError::WantedToken`] if the stream ends early.
    fn expect_token_sequence(&mut self, expected: &[TokenType]) -> Result<(), ParserError> {
        for &kind in expected {
            self.ensure_next_of_kind(kind)?;
        }
        Ok(())
    }

    /// Checks whether the next token has the given `kind` without consuming it.
    ///
    /// Returns `false` if the stream is exhausted.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::LexError`] if the lexer encounters an invalid
    /// token sequence while peeking.
    fn is_peek_token(&mut self, kind: TokenType) -> Result<bool, ParserError> {
        match self.lexer.next() {
            None => Ok(false),
            Some(Err(e)) => Err(ParserError::from(e)),
            Some(Ok(tok)) => {
                let matches = tok.is(kind);
                self.lexer.backtrack().map_err(ParserError::from)?;
                Ok(matches)
            }
        }
    }

    /// Advances the lexer and returns the next token.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::WantedToken`] if the stream is exhausted, or
    /// [`ParserError::LexError`] on a lex failure.
    fn ensure_next_token(&mut self) -> Result<Token, ParserError> {
        self.lexer
            .next()
            .ok_or(ParserError::WantedToken)?
            .map_err(ParserError::from)
    }

    /// Advances the lexer, checks the token kind, and returns the token.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if the token kind doesn't match
    /// `expected`, or propagates errors from [`ensure_next_token`](Self::ensure_next_token).
    fn ensure_next_of_kind(&mut self, expected: TokenType) -> Result<Token, ParserError> {
        let tok = self.ensure_next_token()?;
        if tok.is_not(expected) {
            return Err(ParserError::UnexpectedToken {
                expected,
                found: tok.kind,
            });
        }
        Ok(tok)
    }

    /// Parses `DROP TABLE [IF EXISTS] <name>`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the keyword sequence or table name is missing.
    fn parse_drop(&mut self) -> Result<DropStatement, ParserError> {
        self.expect_token_sequence(&[TokenType::Table])?;
        let if_exists = self.parse_if_exists(false)?;
        let tok = self.ensure_next_of_kind(TokenType::Identifier)?;
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
    fn parse_create(&mut self) -> Result<Statement, ParserError> {
        self.expect_token_sequence(&[TokenType::Table])?;
        let if_not_exists = self.parse_if_exists(true)?;
        let table_name = self.ensure_next_of_kind(TokenType::Identifier)?.value;
        self.ensure_next_of_kind(TokenType::Lparen)?;

        let mut columns: Vec<ColumnDef> = Vec::new();
        let mut primary_key: Option<String> = None;

        loop {
            let curr_tok = self.ensure_next_token()?;
            match curr_tok.kind {
                TokenType::Primary => {
                    self.expect_token_sequence(&[TokenType::Key, TokenType::Lparen])?;
                    let pkey = self.ensure_next_of_kind(TokenType::Identifier)?;
                    self.ensure_next_of_kind(TokenType::Rparen)?;
                    primary_key = Some(pkey.value);
                }
                TokenType::Identifier => {
                    let col_name = curr_tok.value;
                    let type_tok = self.ensure_next_token()?;
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
                                self.expect_token_sequence(&[TokenType::Not, TokenType::Null])?;
                                nullable = false;
                            }
                            TokenType::Primary => {
                                self.expect_token_sequence(&[TokenType::Primary, TokenType::Key])?;
                                is_primary_key = true;
                            }
                            TokenType::AutoIncrement => {
                                self.ensure_next_token()?;
                                auto_increment = true;
                            }
                            TokenType::Default => {
                                self.ensure_next_token()?;
                                let val_tok = self.ensure_next_token()?;
                                default = Some(
                                    Value::try_from(val_tok).map_err(ParserError::ParsingError)?,
                                );
                            }
                            _ => break,
                        }
                    }

                    columns.push(ColumnDef {
                        name: col_name,
                        col_type,
                        nullable,
                        primary_key: is_primary_key,
                        auto_increment,
                        default,
                    });
                }
                _ => {
                    return Err(ParserError::ParsingError(format!(
                        "expected column definition or PRIMARY KEY, got {}",
                        curr_tok.value
                    )));
                }
            }

            match self.ensure_next_token()?.kind {
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
        let if_tok = self.ensure_next_token()?;
        if if_tok.is_not(TokenType::If) {
            self.lexer.backtrack()?;
            return Ok(false);
        }

        if with_not {
            self.expect_token_sequence(&[TokenType::Not, TokenType::Exists])?;
        } else {
            self.expect_token_sequence(&[TokenType::Exists])?;
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
    fn parse_create_index(&mut self) -> Result<CreateIndexStatement, ParserError> {
        self.expect_token_sequence(&[TokenType::Index])?;
        let if_not_exists = self.parse_if_exists(true)?;

        let index_name = self.ensure_next_of_kind(TokenType::Identifier)?;

        self.expect_token_sequence(&[TokenType::Lparen])?;
        let col_name = self.ensure_next_of_kind(TokenType::Identifier)?;
        self.expect_token_sequence(&[TokenType::Rparen])?;

        let index_type = self
            .on_peek_token(TokenType::Using, |p| {
                let type_tok = p.ensure_next_token()?;
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
    fn parse_drop_index(&mut self) -> Result<DropIndexStatement, ParserError> {
        self.expect_token_sequence(&[TokenType::Index])?;
        let if_exists = self.parse_if_exists(false)?;

        let index_name = self.ensure_next_of_kind(TokenType::Identifier)?;
        let table_name = if self.is_peek_token(TokenType::On)? {
            self.ensure_next_token()?;
            String::from(&self.ensure_next_of_kind(TokenType::Identifier)?)
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
    fn parse_show_index(&mut self) -> Result<ShowIndexesStatement, ParserError> {
        self.expect_token_sequence(&[TokenType::Show, TokenType::Indexes])?;

        let table_name = self.is_peek_token(TokenType::From).and_then(|ok| {
            if ok {
                self.ensure_next_token()?;
                let t = self.ensure_next_of_kind(TokenType::Identifier)?;
                Ok(Some(t.value))
            } else {
                Ok(None)
            }
        })?;

        Ok(Statement::show_indexes(table_name))
    }

    /// Parses `<table> [<alias>]`, returning the table name and an optional alias.
    ///
    /// The alias is a bare identifier immediately following the table name.  If
    /// the next token is not an identifier the alias is `None` and the token is
    /// left on the stream.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if no table name identifier is present.
    fn parse_table_with_alias(&mut self) -> Result<(String, Option<String>), ParserError> {
        let table = self.ensure_next_of_kind(TokenType::Identifier)?;
        let alias = if self.is_peek_token(TokenType::Identifier)? {
            Some(String::from(&self.ensure_next_token()?))
        } else {
            None
        };

        Ok((String::from(&table), alias))
    }

    /// Parses `DELETE FROM <table> [<alias>] [WHERE <condition>]`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if `DELETE FROM` or the table name is missing,
    /// or if the `WHERE` clause is malformed.
    fn parse_delete(&mut self) -> Result<DeleteStatement, ParserError> {
        self.expect_token_sequence(&[TokenType::Delete, TokenType::From])?;
        let (table_name, alias) = self.parse_table_with_alias()?;

        if !self.is_peek_token(TokenType::Where)? {
            return Ok(Statement::delete(table_name, alias, None));
        }

        let where_clause = self.on_peek_token(TokenType::Where, super::Parser::parse_where)?;
        Ok(Statement::delete(table_name, alias, where_clause))
    }

    /// Parses `INSERT INTO <table> [(<cols>)] VALUES (<row>)[, (<row>)...]`.
    ///
    /// The column list is optional.  Multiple value rows are supported.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the syntax is malformed or a value token
    /// can't be converted to a [`Value`].
    fn parse_insert(&mut self) -> Result<InsertStatement, ParserError> {
        self.expect_token_sequence(&[TokenType::Insert, TokenType::Into])?;
        let (table_name, _) = self.parse_table_with_alias()?;

        let columns = self.on_peek_token(TokenType::Lparen, |p| {
            p.parse_delimited_list(TokenType::Comma, TokenType::Rparen, |p| {
                let t = p.ensure_next_of_kind(TokenType::Identifier)?;
                Ok(t.value)
            })
        })?;

        self.ensure_next_of_kind(TokenType::Values)?;
        let mut values = Vec::new();
        loop {
            self.ensure_next_of_kind(TokenType::Lparen)?;
            let row = self.parse_delimited_list(TokenType::Comma, TokenType::Rparen, |p| {
                let t = p.ensure_next_token()?;
                Value::try_from(t).map_err(ParserError::ParsingError)
            })?;
            values.push(row);
            if self.on_peek_token(TokenType::Comma, |_| Ok(()))?.is_none() {
                break;
            }
        }

        Ok(Statement::insert(table_name, columns, values))
    }

    /// Parses `UPDATE <table> [<alias>] SET <col>=<val>[, ...] [WHERE <condition>]`.
    ///
    /// Only `=` is accepted as the assignment operator.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if any keyword is missing, a non-`=` operator is
    /// used, or a value token can't be converted to a [`Value`].
    fn parse_update(&mut self) -> Result<UpdateStatement, ParserError> {
        self.ensure_next_of_kind(TokenType::Update)?;
        let (table_name, alias) = self.parse_table_with_alias()?;
        self.ensure_next_of_kind(TokenType::Set)?;

        let mut assignments = Vec::new();
        loop {
            let field = self.ensure_next_of_kind(TokenType::Identifier)?;
            let op = self.ensure_next_of_kind(TokenType::Operator)?;

            if op.value.ne("=") {
                return Err(ParserError::unexpected(TokenType::Operator, op.kind));
            }

            let t = self.ensure_next_token()?;
            let val = Value::try_from(t).map_err(ParserError::ParsingError)?;

            assignments.push(Assignment {
                column: field.value,
                value: val,
            });

            if self.on_peek_token(TokenType::Comma, |_| Ok(()))?.is_none() {
                break;
            }
        }

        let where_clause = self.on_peek_token(TokenType::Where, super::Parser::parse_where)?;
        Ok(Statement::update(
            table_name,
            alias,
            assignments,
            where_clause,
        ))
    }

    /// Parses a full `SELECT` statement including all optional clauses.
    ///
    /// Grammar (simplified):
    /// ```text
    /// SELECT [DISTINCT] (* | <expr>[, ...])
    ///   FROM <table> [<alias>]
    ///   [<join> ...]
    ///   [WHERE <condition>]
    ///   [GROUP BY <col>]
    ///   [ORDER BY <col> [ASC|DESC]]
    ///   [LIMIT <n> [OFFSET <m>]]
    /// ```
    ///
    /// Aggregate functions (`COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`) are
    /// supported in the select list.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if any required token is missing or an aggregate
    /// function name is unrecognized.
    fn parse_select(&mut self) -> Result<SelectStatement, ParserError> {
        self.ensure_next_of_kind(TokenType::Select)?;

        let distinct = self
            .on_peek_token(TokenType::Distinct, |_p| Ok(true))?
            .unwrap_or(false);

        let columns = if self.is_peek_token(TokenType::Asterisk)? {
            self.ensure_next_of_kind(TokenType::Asterisk)?;
            SelectColumns::All
        } else {
            let select_expressions =
                self.parse_delimited_list(TokenType::Comma, TokenType::From, |p| {
                    let name_tok = p.ensure_next_of_kind(TokenType::Identifier)?;
                    let name = name_tok.value.to_uppercase();

                    if p.is_peek_token(TokenType::Lparen)? {
                        p.ensure_next_of_kind(TokenType::Lparen)?;

                        if name == "COUNT" && p.is_peek_token(TokenType::Asterisk)? {
                            p.ensure_next_of_kind(TokenType::Asterisk)?;
                            p.ensure_next_of_kind(TokenType::Rparen)?;
                            return Ok(SelectExpr::CountStar);
                        }

                        let agg =
                            AggFunc::try_from(name.as_str()).map_err(ParserError::ParsingError)?;

                        let col_tok = p.ensure_next_of_kind(TokenType::Identifier)?;
                        p.ensure_next_of_kind(TokenType::Rparen)?;
                        Ok(SelectExpr::Agg(agg, col_tok.value))
                    } else {
                        Ok(SelectExpr::Column(name_tok.value))
                    }
                })?;
            SelectColumns::Exprs(select_expressions)
        };

        self.ensure_next_of_kind(TokenType::From)?;
        let (table_name, alias) = self.parse_table_with_alias()?;

        let joins = self.parse_joins()?;

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;

        let group_by = self
            .on_peek_token(TokenType::Group, |p| {
                p.ensure_next_of_kind(TokenType::By)?;
                let column = p.ensure_next_of_kind(TokenType::Identifier)?;
                Ok(vec![column.value])
            })?
            .unwrap_or_default();

        let order_by = self.on_peek_token(TokenType::Order, |p| {
            p.ensure_next_of_kind(TokenType::By)?;
            let order_col = p.ensure_next_of_kind(TokenType::Identifier)?;

            let dir = if p.is_peek_token(TokenType::Desc)? {
                p.ensure_next_of_kind(TokenType::Desc)?;
                OrderDirection::Desc
            } else {
                p.on_peek_token(TokenType::Asc, |_p| Ok(()))?;
                OrderDirection::Asc
            };

            Ok(OrderBy(order_col.value, dir))
        })?;

        let limit_offset = self.on_peek_token(TokenType::Limit, |p| {
            let limit = p.parse_int("limit")?;

            let offset = p
                .on_peek_token(TokenType::Offset, |p| p.parse_int("offset"))?
                .unwrap_or(0);

            Ok((limit, offset))
        })?;

        Ok(SelectStatement {
            distinct,
            columns,
            table_name,
            alias,
            joins,
            where_clause,
            group_by,
            having: None,
            order_by,
            limit_offset,
        })
    }

    /// Parses zero or more JOIN clauses following a `FROM` target.
    ///
    /// Supports `[INNER] JOIN`, `LEFT JOIN`, and `RIGHT JOIN`.  Each clause
    /// must include an `ON <condition>` predicate.  Parsing stops when no
    /// recognized join keyword is found next.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if a join keyword is present but the rest of the
    /// clause (`JOIN <table> ON <condition>`) is malformed.
    fn parse_joins(&mut self) -> Result<Vec<Join>, ParserError> {
        let mut joins = vec![];

        loop {
            let kind = if self.is_peek_token(TokenType::Inner)? {
                self.ensure_next_of_kind(TokenType::Inner)?;
                JoinKind::Inner
            } else if self.is_peek_token(TokenType::Left)? {
                self.ensure_next_of_kind(TokenType::Left)?;
                JoinKind::Left
            } else if self.is_peek_token(TokenType::Right)? {
                self.ensure_next_of_kind(TokenType::Right)?;
                JoinKind::Right
            } else if self.is_peek_token(TokenType::Join)? {
                JoinKind::Inner
            } else {
                break;
            };

            self.ensure_next_of_kind(TokenType::Join)?;
            let (table, alias) = self.parse_table_with_alias()?;
            self.ensure_next_of_kind(TokenType::On)?;
            let on = self.parse_where()?;

            joins.push(Join {
                kind,
                table,
                alias,
                on,
            });
        }

        Ok(joins)
    }

    /// Consumes the next token and parses it as a non-negative integer.
    ///
    /// `name` is used only in the error message to identify which integer was
    /// expected (e.g. `"limit"` or `"offset"`).
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if the token is not an
    /// [`TokenType::Int`], or [`ParserError::ParsingError`] if the integer
    /// value overflows `u64`.
    fn parse_int(&mut self, name: &str) -> Result<u64, ParserError> {
        let tok = self.ensure_next_of_kind(TokenType::Int)?;
        tok.value
            .parse::<u64>()
            .map_err(|e| ParserError::ParsingError(format!("invalid {name}: {e}")))
    }

    /// Runs `exec` only if the next token matches `expected`, consuming nothing
    /// when it doesn't.
    ///
    /// Returns `Some(T)` when `exec` ran successfully, or `None` when the peek
    /// didn't match.  The token is never consumed by this method itself — `exec`
    /// is responsible for consuming the tokens it needs.
    ///
    /// # Errors
    ///
    /// Returns any error produced by the peek check or by `exec`.
    fn on_peek_token<T, K>(
        &mut self,
        expected: TokenType,
        mut exec: K,
    ) -> Result<Option<T>, ParserError>
    where
        T: Sized,
        K: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        self.is_peek_token(expected).and_then(|ok| {
            if !ok {
                return Ok(None);
            }
            self.ensure_next_token()?;
            let t = exec(self)?;
            Ok(Some(t))
        })
    }

    /// Parses `WHERE condition`, supporting `AND` (higher precedence) and `OR` (lower precedence).
    ///
    /// Strategy: collect predicates into AND-groups separated by OR, then fold both levels.
    /// e.g. `a=1 OR b=2 AND c=3` → `a=1 OR (b=2 AND c=3)`
    fn parse_where(&mut self) -> Result<WhereCondition, ParserError> {
        let mut or_groups = vec![vec![self.parse_predicate()?]];

        loop {
            if let Some(pred) = self.on_peek_token(TokenType::And, Self::parse_predicate)? {
                or_groups.last_mut().unwrap().push(pred);
            } else if let Some(pred) = self.on_peek_token(TokenType::Or, Self::parse_predicate)? {
                or_groups.push(vec![pred]);
            } else {
                break;
            }
        }

        let or_terms = or_groups.into_iter().map(|group| {
            group
                .into_iter()
                .reduce(|l, r| WhereCondition::And(Box::new(l), Box::new(r)))
                .unwrap()
        });

        Ok(or_terms
            .reduce(|l, r| WhereCondition::Or(Box::new(l), Box::new(r)))
            .unwrap())
    }

    /// Parses a single `field op value` predicate.
    /// Parses a single `<field> <op> <value>` predicate.
    ///
    /// The value token is interpreted as an `i64` integer when it parses as
    /// one, otherwise it is stored as a string literal.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the field or operator tokens are missing, or
    /// the operator string is not a recognized [`Predicate`] variant.
    fn parse_predicate(&mut self) -> Result<WhereCondition, ParserError> {
        let field = self.ensure_next_of_kind(TokenType::Identifier)?;
        let op = self.ensure_next_of_kind(TokenType::Operator)?;
        let op = Predicate::try_from(op.value.as_str()).map_err(ParserError::ParsingError)?;

        let val_tok = self.ensure_next_token()?;
        let value = if let Ok(n) = val_tok.value.parse::<i64>() {
            Literal::Int(n)
        } else {
            Literal::Str(val_tok.value.clone())
        };

        Ok(WhereCondition::predicate(field.value, op, value))
    }

    /// Parses a non-empty list of items separated by `delimiter` and ended by
    /// `terminator`, calling `parse` for each item.
    ///
    /// Both the `delimiter` and `terminator` tokens are consumed.  The
    /// `terminator` token is not included in the returned `Vec`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if `parse` fails, the list is empty, or a token
    /// that is neither `delimiter` nor `terminator` appears where one was
    /// expected.
    fn parse_delimited_list<T, W>(
        &mut self,
        delimiter: TokenType,
        terminator: TokenType,
        mut parse: W,
    ) -> Result<Vec<T>, ParserError>
    where
        T: Sized,
        W: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        let mut items = Vec::new();
        loop {
            let item = parse(self)?;
            items.push(item);

            let t = self.ensure_next_token()?;
            match t.kind {
                _ if t.is(delimiter) => {}
                _ if t.is(terminator) => break,
                _ => {
                    return Err(ParserError::ParsingError(format!(
                        "expected {:?} or {:?}, got {}",
                        delimiter, terminator, t.value
                    )));
                }
            }
        }
        Ok(items)
    }
}
