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

use super::Parser;
use crate::{
    Value,
    parser::{
        lexer::LexError,
        statements::{ColumnRef, Statement, WhereCondition},
        token::{Token, TokenType},
    },
    primitives::Predicate,
};

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

        let stmt = match tok.kind {
            TokenType::Delete => Statement::Delete(self.parse_delete()?),
            TokenType::Insert => Statement::Insert(self.parse_insert()?),
            TokenType::Update => Statement::Update(self.parse_update()?),
            TokenType::Show => Statement::ShowIndexes(self.parse_show_index()?),
            TokenType::Select => Statement::Select(self.parse_select()?),
            TokenType::Drop => {
                self.bump()?; // consume DROP
                let next = self.bump()?;
                self.lexer.backtrack()?; // put TABLE/INDEX back

                match next.kind {
                    TokenType::Index => Statement::DropIndex(self.parse_drop_index()?),
                    TokenType::Table => Statement::Drop(self.parse_drop()?),
                    _ => {
                        return Err(ParserError::ParsingError(format!(
                            "expected TABLE or INDEX after DROP, got {}",
                            next.value
                        )));
                    }
                }
            }
            TokenType::Create => {
                self.bump()?;
                let next = self.bump()?;
                self.lexer.backtrack()?;

                match next.kind {
                    TokenType::Index => Statement::CreateIndex(self.parse_create_index()?),
                    TokenType::Table => Statement::CreateTable(self.parse_create()?),
                    _ => {
                        return Err(ParserError::ParsingError(format!(
                            "expected TABLE or INDEX after CREATE, got {}",
                            next.value
                        )));
                    }
                }
            }
            TokenType::Alter => Statement::AlterTable(self.parse_alter_table()?),
            _ => {
                return Err(ParserError::ParsingError(format!(
                    "unexpected statement start: {}",
                    tok.value
                )));
            }
        };

        self.expect_end()?;
        Ok(stmt)
    }

    /// Consumes an optional trailing `;` and asserts the token stream is exhausted.
    ///
    /// Used at the end of [`Parser::parse`] to reject extra tokens after a
    /// successful statement parse — `SELECT 1 garbage` should fail, not silently
    /// truncate. Multi-statement input would need a separate API that loops
    /// until EOF; `parse` is strictly single-statement.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::ParsingError`] when an unexpected token follows
    /// the statement (after an optional trailing `;`), or propagates a
    /// [`LexError`] from the lexer.
    fn expect_end(&mut self) -> Result<(), ParserError> {
        self.if_peek_then_consume(TokenType::Semicolon)?;

        match self.lexer.next() {
            None => Ok(()),
            Some(Err(e)) => Err(ParserError::from(e)),
            Some(Ok(tok)) => Err(ParserError::ParsingError(format!(
                "unexpected token {tok} after end of statement"
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

    /// Checks whether the next token has the given `kind` and consumes it if it does.
    ///
    /// Returns `false` if the stream is exhausted.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::LexError`] if the lexer encounters an invalid
    /// token sequence while peeking.
    fn if_peek_then_consume(&mut self, kind: TokenType) -> Result<bool, ParserError> {
        let matches = self.peek_is(kind)?;
        if matches {
            self.bump()?;
        }
        Ok(matches)
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

    /// Parses `<table> [[AS] <alias>]`, returning the table name and an optional alias.
    ///
    /// The alias may be introduced by an explicit `AS` keyword or appear as a
    /// bare identifier immediately following the table name. When `AS` is
    /// present, an identifier must follow it.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if no table name identifier is present, or if
    /// `AS` is not followed by an identifier.
    fn parse_table_with_alias(&mut self) -> Result<(String, Option<String>), ParserError> {
        let table = self.expect(TokenType::Identifier)?;

        let saw_as = self.on_peek_token(TokenType::As, |_| Ok(()))?.is_some();

        let alias = if saw_as {
            Some(String::from(&self.expect(TokenType::Identifier)?))
        } else if self.peek_is(TokenType::Identifier)? {
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

    /// Parses a single `<field> <op> <value>` predicate.
    ///
    /// The value token is converted to a [`Value`] via [`Value::try_from`],
    /// which handles integer, float, string, `NULL`, and boolean literals.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the field or operator tokens are missing,
    /// the operator string is not a recognized [`Predicate`] variant, or the
    /// value token cannot be interpreted as a literal.
    fn parse_predicate(&mut self) -> Result<WhereCondition, ParserError> {
        let field = self.parse_column_ref()?;
        let op = self.expect(TokenType::Operator)?;
        let op = Predicate::try_from(op.value.as_str()).map_err(ParserError::ParsingError)?;

        let val_tok = self.bump()?;
        let value = Value::try_from(val_tok).map_err(ParserError::ParsingError)?;

        Ok(WhereCondition::predicate(field, op, value))
    }

    /// Parses a possibly qualified column reference, e.g., `col` or `tbl.col`.
    ///
    /// Handles a column identifier, optionally preceded by a qualifier and dot
    /// (e.g., `table.col`). The method expects either:
    ///   - A bare column name: consumes a single identifier token as the column name.
    ///   - A qualified column name: expects a dot, then two identifier tokens (qualifier and column
    ///     name).
    ///
    /// # Returns
    /// - [`Ok(ColumnRef)`]: with the parsed qualifier (if present) and column name.
    /// - [`Err(ParserError)`]: if any identifier token is missing or malformed.
    pub(super) fn parse_column_ref(&mut self) -> Result<ColumnRef, ParserError> {
        let first = self.expect(TokenType::Identifier)?.value;
        if self.if_peek_then_consume(TokenType::Dot)? {
            let name = self.expect(TokenType::Identifier)?.value;
            Ok(ColumnRef {
                qualifier: Some(first),
                name,
            })
        } else {
            Ok(ColumnRef {
                qualifier: None,
                name: first,
            })
        }
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

    /// Parses a comma-separated sequence of items.
    ///
    /// This method expects the first item to be present (i.e., the sequence is not empty).
    /// It uses the provided `item` closure to parse each element. After parsing each item,
    /// it checks for a comma; if a comma is found, parsing continues, otherwise the process stops.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if parsing any item fails.
    ///
    /// # Example
    ///
    /// Used to parse lists like column definitions: `a, b, c`
    fn parse_comma_sep<T, F>(&mut self, mut item: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        let mut out = vec![item(self)?];
        while self.if_peek_then_consume(TokenType::Comma)? {
            out.push(item(self)?);
        }
        Ok(out)
    }

    /// Parses a construct enclosed in parentheses.
    ///
    /// This method expects the next token to be `'('`, then parses the content using the given
    /// `inner` closure, and finally expects a closing `')'`. The result of parsing the inner
    /// content is returned.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the opening or closing parenthesis is missing,
    /// or if parsing the inner content fails.
    ///
    /// # Example
    ///
    /// Used to parse parenthesized lists or expressions, e.g. `(a, b, c)`
    fn parens<T, F>(&mut self, inner: F) -> Result<T, ParserError>
    where
        F: FnOnce(&mut Self) -> Result<T, ParserError>,
    {
        self.expect(TokenType::Lparen)?;
        let v = inner(self)?;
        self.expect(TokenType::Rparen)?;
        Ok(v)
    }

    /// Expects the next token to be an identifier and returns its value as a `String`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the next token is not an identifier.
    #[inline]
    fn expect_ident(&mut self) -> Result<String, ParserError> {
        Ok(self.expect(TokenType::Identifier)?.value)
    }

    /// Parses a parenthesized, comma-separated list of items.
    ///
    /// This method expects the next token to be `'('`, then repeatedly parses items using the given
    /// `item` closure separated by commas, and finally expects a closing `')'`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the opening or closing parenthesis is missing,
    /// if any item fails to parse, or if the comma separation is malformed.
    ///
    /// # Example
    ///
    /// Used to parse lists such as column definitions or value tuples:
    /// `(a, b, c)`
    fn paren_list<T, F>(&mut self, item: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Self) -> Result<T, ParserError>,
    {
        self.parens(|p| p.parse_comma_sep(item))
    }
}
