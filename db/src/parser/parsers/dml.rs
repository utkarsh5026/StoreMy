use crate::{
    Value,
    parser::{
        Parser,
        statements::{Assignment, DeleteStatement, InsertStatement, Statement, UpdateStatement},
        token::TokenType,
    },
};

use super::ParserError;

impl Parser {
    /// Parses `DELETE FROM <table> [<alias>] [WHERE <condition>]`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if `DELETE FROM` or the table name is missing,
    /// or if the `WHERE` clause is malformed.
    pub(super) fn parse_delete(&mut self) -> Result<DeleteStatement, ParserError> {
        self.expect_token_sequence(&[TokenType::Delete, TokenType::From])?;
        let (table_name, alias) = self.parse_table_with_alias()?;

        if !self.is_peek_token(TokenType::Where)? {
            return Ok(Statement::delete(table_name, alias, None));
        }

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;
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
    pub(super) fn parse_insert(&mut self) -> Result<InsertStatement, ParserError> {
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
    pub(super) fn parse_update(&mut self) -> Result<UpdateStatement, ParserError> {
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

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;
        Ok(Statement::update(
            table_name,
            alias,
            assignments,
            where_clause,
        ))
    }
}
