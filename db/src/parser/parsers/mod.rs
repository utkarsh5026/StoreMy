//! SQL statement parser.
//!
//! This module implements a recursive-descent parser that turns a stream of
//! [`Token`]s produced by [`crate::parser::lexer`] into typed [`Statement`]
//! AST nodes.  Each `parse_*` method handles one grammar production.
//!
//! The entry point is [`Parser::parse`], which dispatches to the appropriate
//! sub-parser based on the first keyword in the input.

mod ddl;
mod dml;
mod query;

use thiserror::Error;

use crate::{
    Value,
    parser::{
        lexer::LexError,
        statements::{Statement, WhereCondition},
        token::{Token, TokenType},
    },
    primitives::Predicate,
};

use super::Parser;

/// Errors that can occur while parsing a SQL statement.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    #[error("wanted token but stream was exhausted")]
    WantedToken,

    #[error("expected {expected}, but found {found:?}")]
    UnexpectedToken {
        expected: TokenType,
        found: TokenType,
    },

    #[error("parsing error from {0}")]
    ParsingError(String),

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
        let tok = self.bump()?;
        self.lexer.backtrack()?;

        match tok.kind {
            TokenType::Delete => Ok(Statement::Delete(self.parse_delete()?)),
            TokenType::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            TokenType::Update => Ok(Statement::Update(self.parse_update()?)),
            TokenType::Show => Ok(Statement::ShowIndexes(self.parse_show_index()?)),
            TokenType::Select => Ok(Statement::Select(self.parse_select()?)),
            TokenType::Drop => {
                self.bump()?; // consume DROP
                let next = self.bump()?;
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
                self.bump()?;
                let next = self.bump()?;
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
    fn expect_seq(&mut self, expected: &[TokenType]) -> Result<(), ParserError> {
        for &kind in expected {
            self.expect(kind)?;
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
    fn peek_is(&mut self, kind: TokenType) -> Result<bool, ParserError> {
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
    fn bump(&mut self) -> Result<Token, ParserError> {
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
    /// `expected`, or propagates errors from [`bump`](Self::bump).
    fn expect(&mut self, expected: TokenType) -> Result<Token, ParserError> {
        let tok = self.bump()?;
        if tok.is_not(expected) {
            return Err(ParserError::UnexpectedToken {
                expected,
                found: tok.kind,
            });
        }
        Ok(tok)
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
        let table = self.expect(TokenType::Identifier)?;
        let alias = if self.peek_is(TokenType::Identifier)? {
            Some(String::from(&self.bump()?))
        } else {
            None
        };

        Ok((String::from(&table), alias))
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
        self.peek_is(expected).and_then(|ok| {
            if !ok {
                return Ok(None);
            }
            self.bump()?;
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
        let field = self.expect(TokenType::Identifier)?;
        let op = self.expect(TokenType::Operator)?;
        let op = Predicate::try_from(op.value.as_str()).map_err(ParserError::ParsingError)?;

        let val_tok = self.bump()?;
        let value = if let Ok(n) = val_tok.value.parse::<i64>() {
            Value::Int64(n)
        } else {
            Value::String(val_tok.value.clone())
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

            let t = self.bump()?;
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
