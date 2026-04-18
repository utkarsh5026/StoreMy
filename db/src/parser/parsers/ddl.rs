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

use super::ParserError;

impl Parser {
    /// Parses `DROP TABLE [IF EXISTS] <name>`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the keyword sequence or table name is missing.
    pub(super) fn parse_drop(&mut self) -> Result<DropStatement, ParserError> {
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
    pub(super) fn parse_create(&mut self) -> Result<Statement, ParserError> {
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
    pub(super) fn parse_create_index(&mut self) -> Result<CreateIndexStatement, ParserError> {
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
    pub(super) fn parse_drop_index(&mut self) -> Result<DropIndexStatement, ParserError> {
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
    pub(super) fn parse_show_index(&mut self) -> Result<ShowIndexesStatement, ParserError> {
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
}
