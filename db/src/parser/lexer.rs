//! Lexical analysis for SQL input.
//!
//! This module turns a raw SQL string into a flat stream of [`Token`]s that
//! the parser can consume one at a time.  The entry point is [`Lexer::new`];
//! iterate over it with a `for` loop or collect the results.
//!
//! # Supported token kinds
//!
//! - Integer literals (`42`, `0`)
//! - String literals in single or double quotes (`'hello'`, `"world"`)
//! - Identifiers and SQL keywords (`SELECT`, `FROM`, `user_id`, …)
//! - Comparison operators: `=`, `<`, `>`, `!`, `<=`, `>=`, `!=`, `<>`
//! - Punctuation: `,`, `;`, `(`, `)`, `*`
//!
//! Whitespace is skipped silently between tokens.

use crate::parser::token::{Token, TokenType};

use thiserror::Error;

/// Errors that can occur while lexing a SQL string.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum LexError {
    /// A character was found that is not part of any valid SQL token.
    #[error("invalid character '{ch}' found at position {position}")]
    InvalidCharacter {
        /// The offending character.
        ch: char,
        /// Byte offset in the original input string.
        position: usize,
    },

    /// The lexer expected a particular token kind but found something else.
    #[error("expected {expected}, but found {found:?} at position {position}")]
    UnexpectedToken {
        /// Description of what was expected (e.g. `"closing parenthesis"`).
        expected: String,
        /// The token kind that was actually found.
        found: super::token::TokenType,
        /// Byte offset in the original input string.
        position: usize,
    },

    /// The input ended before a required token was found.
    #[error("unexpected end of file: expected {expected}")]
    UnexpectedEof {
        /// Description of what was expected (e.g. `"closing quote '''"`).
        expected: String,
    },

    /// An identifier was parsed but does not match any known keyword.
    #[error("unrecognized keyword: {0}")]
    UnrecognizedKeyword(String),

    /// [`Lexer::backtrack`] was called when there is no previous token to return to.
    #[error("cant backtrack")]
    BackTrack,
}

/// A single-pass SQL lexer.
///
/// `Lexer` holds the input as a `Vec<char>` and walks through it one character
/// at a time, emitting [`Token`]s on demand.  It also keeps track of the most
/// recently emitted token so that the parser can call [`backtrack`](Self::backtrack)
/// to re-read it.
///
/// Use [`Lexer::new`] to create one, then either call [`next_token`](Self::next_token)
/// in a loop or use the [`Iterator`] impl to drive it.
pub(super) struct Lexer {
    /// The full input broken into individual Unicode scalar values.
    input: Vec<char>,
    /// Current read position (index into `input`).
    position: usize,
    /// Total number of characters in `input` (cached to avoid repeated `.len()` calls).
    length: usize,
    /// The last token returned by [`next_token`](Self::next_token), used by
    /// [`backtrack`](Self::backtrack) to reset `position`.
    curr_token: Option<Token>,
}

impl Lexer {
    /// Creates a new lexer for the given SQL string.
    ///
    /// The string is immediately copied into an owned `Vec<char>` so that
    /// character-level indexing is O(1).
    pub fn new(input: &str) -> Self {
        Lexer {
            input: input.chars().collect(),
            position: 0,
            length: input.len(),
            curr_token: None,
        }
    }

    /// Builds a [`Token`] from its components and saves it as `curr_token`.
    ///
    /// Every path that produces a token (except string and operator literals)
    /// goes through here so that `backtrack` always has something to restore.
    fn create_token(&mut self, kind: TokenType, val: String, pos: usize) -> Token {
        self.curr_token = Some((kind, val.clone(), pos).into());
        (kind, val, pos).into()
    }

    /// Reads and returns the next token from the input, or `None` at end-of-input.
    ///
    /// Leading whitespace is consumed silently before each token.  The lexer
    /// dispatches on the first character to decide which production to apply:
    ///
    /// - `=`, `<`, `>`, `!` → operator (possibly two characters, e.g. `<=`)
    /// - `'` or `"` → quoted string literal
    /// - ASCII digit → integer literal
    /// - ASCII letter or `_` → keyword or identifier
    /// - `,`, `;`, `(`, `)`, `*` → single-character punctuation
    ///
    /// # Errors
    ///
    /// Returns [`LexError::InvalidCharacter`] if the current character does not
    /// start any recognized token.  Returns [`LexError::UnexpectedEof`] if a
    /// quoted string is never closed.
    pub fn next_token(&mut self) -> Result<Option<Token>, LexError> {
        self.read_while(char::is_whitespace, false);
        if self.position >= self.length {
            return Ok(None);
        }

        let start = self.position;
        let ch = self.input.get(start).unwrap();

        match ch {
            '=' | '<' | '>' | '!' => Ok(Some(
                (TokenType::Operator, self.read_operator(), start).into(),
            )),
            '\'' | '"' => {
                let s = self.read_string()?;
                Ok(Some((TokenType::String, s, start).into()))
            }
            c if c.is_ascii_digit() => {
                let num = self.read_while(|c| c.is_ascii_digit(), true).unwrap();
                Ok(Some(self.create_token(TokenType::Int, num, start)))
            }
            c if c.is_ascii_alphabetic() || *c == '_' => {
                let ident = self
                    .read_while(|ch| ch.is_alphanumeric() || ch == '_', true)
                    .unwrap();
                let token_type = match ident.parse::<TokenType>() {
                    Ok(t) => t,
                    Err(_) => TokenType::Identifier,
                };
                Ok(Some(self.create_token(token_type, ident, start)))
            }
            ',' | ';' | '(' | ')' | '*' => {
                self.position += 1;
                let token_type = match ch {
                    ',' => TokenType::Comma,
                    ';' => TokenType::Semicolon,
                    '(' => TokenType::Lparen,
                    ')' => TokenType::Rparen,
                    '*' => TokenType::Asterisk,
                    _ => unreachable!(),
                };
                Ok(Some(self.create_token(token_type, ch.to_string(), start)))
            }

            _ => Err(LexError::InvalidCharacter {
                ch: *ch,
                position: start,
            }),
        }
    }

    /// Rewinds the lexer to the start of the most recently emitted token.
    ///
    /// After a successful call the next [`next_token`](Self::next_token) will
    /// re-emit the same token.  This is a single-level undo: calling `backtrack`
    /// twice in a row without an intervening `next_token` returns an error.
    ///
    /// # Errors
    ///
    /// Returns [`LexError::BackTrack`] if no token has been emitted yet (i.e.
    /// `next_token` has never been called, or the lexer was just constructed).
    pub fn backtrack(&mut self) -> Result<(), LexError> {
        match &self.curr_token {
            Some(tok) => {
                self.position = tok.position;
                Ok(())
            }
            None => Err(LexError::BackTrack),
        }
    }

    /// Reads one or two characters as a comparison operator and advances `position`.
    ///
    /// Two-character operators (`<=`, `>=`, `!=`, `<>`) are consumed in full.
    /// All other first characters (`=`, isolated `<`, `>`, `!`) are returned as-is.
    fn read_operator(&mut self) -> String {
        let first = self.input[self.position];
        self.position += 1;

        if let Some(next) = self.input.get(self.position) {
            match (first, next) {
                ('<' | '>' | '!', '=') | ('<', '>') => {
                    self.position += 1;
                    format!("{first}{next}")
                }
                _ => first.to_string(),
            }
        } else {
            first.to_string()
        }
    }

    /// Reads a quoted string literal and returns its contents without the surrounding quotes.
    ///
    /// The opening quote character (single or double) determines what closes the
    /// string.  The lexer starts just before the opening quote and leaves
    /// `position` pointing at the character after the closing quote.
    ///
    /// # Errors
    ///
    /// Returns [`LexError::UnexpectedEof`] if the input ends before the closing
    /// quote is found.
    fn read_string(&mut self) -> Result<String, LexError> {
        let quote_char = self.input[self.position];
        self.position += 1;
        let mut value = String::new();

        while self.position < self.length {
            let curr = self.input[self.position];
            if curr == quote_char {
                self.position += 1;
                return Ok(value);
            }
            value.push(curr);
            self.position += 1;
        }

        Err(LexError::UnexpectedEof {
            expected: format!("closing quote '{quote_char}'"),
        })
    }

    /// Advances `position` as long as `condition` holds for the current character.
    ///
    /// When `collect` is `true` the consumed characters are gathered into a
    /// `String` and returned as `Some(String)`.  When `collect` is `false` the
    /// characters are discarded and `None` is returned (useful for skipping
    /// whitespace without allocating).
    fn read_while(&mut self, condition: impl Fn(char) -> bool, collect: bool) -> Option<String> {
        let mut result = String::new();
        while self.position < self.length && condition(self.input[self.position]) {
            if collect {
                result.push(self.input[self.position]);
            }
            self.position += 1;
        }

        if collect { Some(result) } else { None }
    }
}

/// Drives the lexer as a standard Rust iterator.
///
/// Each call to `next` returns `Some(Ok(token))` for a successfully lexed
/// token, `Some(Err(e))` when a lex error occurs, and `None` once the entire
/// input has been consumed.  The iterator stops naturally at end-of-input; it
/// does *not* short-circuit after an error — the caller is responsible for
/// checking each `Result`.
impl Iterator for Lexer {
    type Item = Result<super::token::Token, LexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.length {
            return None;
        }

        self.next_token().transpose() // Convert Result<Option<Token>, LexError> to Option<Result<Token, LexError>>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::token::TokenType;

    fn lex(input: &str) -> Vec<Token> {
        Lexer::new(input).map(|r| r.unwrap()).collect()
    }

    fn try_lex(input: &str) -> Result<Vec<Token>, LexError> {
        Lexer::new(input).collect()
    }

    fn tok(kind: TokenType, value: &str, position: usize) -> Token {
        Token {
            kind,
            value: value.to_string(),
            position,
        }
    }

    #[test]
    fn empty_input_yields_no_tokens() {
        assert!(lex("").is_empty());
    }

    #[test]
    fn whitespace_only_yields_no_tokens() {
        assert!(lex("   \t\n  ").is_empty());
    }

    #[test]
    fn single_integer() {
        assert_eq!(lex("42"), vec![tok(TokenType::Int, "42", 0)]);
    }

    #[test]
    fn integer_with_surrounding_whitespace() {
        assert_eq!(lex("  99  "), vec![tok(TokenType::Int, "99", 2)]);
    }

    #[test]
    fn multiple_integers() {
        assert_eq!(
            lex("1 22 333"),
            vec![
                tok(TokenType::Int, "1", 0),
                tok(TokenType::Int, "22", 2),
                tok(TokenType::Int, "333", 5),
            ]
        );
    }

    #[test]
    fn single_quoted_string() {
        assert_eq!(lex("'hello'"), vec![tok(TokenType::String, "hello", 0)]);
    }

    #[test]
    fn double_quoted_string() {
        assert_eq!(lex("\"world\""), vec![tok(TokenType::String, "world", 0)]);
    }

    #[test]
    fn string_with_spaces_inside() {
        assert_eq!(
            lex("'hello world'"),
            vec![tok(TokenType::String, "hello world", 0)]
        );
    }

    #[test]
    fn empty_string_literal() {
        assert_eq!(lex("''"), vec![tok(TokenType::String, "", 0)]);
    }

    #[test]
    fn unterminated_single_quoted_string() {
        assert_eq!(
            try_lex("'oops").unwrap_err(),
            LexError::UnexpectedEof {
                expected: "closing quote '''".to_string()
            }
        );
    }

    #[test]
    fn unterminated_double_quoted_string() {
        assert_eq!(
            try_lex("\"oops").unwrap_err(),
            LexError::UnexpectedEof {
                expected: "closing quote '\"'".to_string()
            }
        );
    }

    #[test]
    fn operator_equals() {
        assert_eq!(lex("="), vec![tok(TokenType::Operator, "=", 0)]);
    }

    #[test]
    fn operator_less_than() {
        assert_eq!(lex("<"), vec![tok(TokenType::Operator, "<", 0)]);
    }

    #[test]
    fn operator_greater_than() {
        assert_eq!(lex(">"), vec![tok(TokenType::Operator, ">", 0)]);
    }

    #[test]
    fn operator_exclamation_alone() {
        assert_eq!(lex("!"), vec![tok(TokenType::Operator, "!", 0)]);
    }

    #[test]
    fn operator_less_equal() {
        assert_eq!(lex("<="), vec![tok(TokenType::Operator, "<=", 0)]);
    }

    #[test]
    fn operator_greater_equal() {
        assert_eq!(lex(">="), vec![tok(TokenType::Operator, ">=", 0)]);
    }

    #[test]
    fn operator_not_equal_bang() {
        assert_eq!(lex("!="), vec![tok(TokenType::Operator, "!=", 0)]);
    }

    #[test]
    fn operator_not_equal_sql() {
        assert_eq!(lex("<>"), vec![tok(TokenType::Operator, "<>", 0)]);
    }

    #[test]
    fn double_operator_consumes_both_chars() {
        // position after `<=` should be 2, leaving `42` at position 2
        let tokens = lex("<= 42");
        assert_eq!(
            tokens,
            vec![
                tok(TokenType::Operator, "<=", 0),
                tok(TokenType::Int, "42", 3),
            ]
        );
    }

    #[test]
    fn equals_followed_by_non_operator_stays_single() {
        // `=a` — the `=` should not swallow `a`
        let tokens = lex("= foo");
        assert_eq!(tokens[0], tok(TokenType::Operator, "=", 0));
        assert_eq!(tokens[1].kind, TokenType::Identifier);
    }

    #[test]
    fn comma() {
        assert_eq!(lex(","), vec![tok(TokenType::Comma, ",", 0)]);
    }

    #[test]
    fn semicolon() {
        assert_eq!(lex(";"), vec![tok(TokenType::Semicolon, ";", 0)]);
    }

    #[test]
    fn lparen() {
        assert_eq!(lex("("), vec![tok(TokenType::Lparen, "(", 0)]);
    }

    #[test]
    fn rparen() {
        assert_eq!(lex(")"), vec![tok(TokenType::Rparen, ")", 0)]);
    }

    #[test]
    fn asterisk() {
        assert_eq!(lex("*"), vec![tok(TokenType::Asterisk, "*", 0)]);
    }

    #[test]
    fn keyword_select_uppercase() {
        assert_eq!(lex("SELECT"), vec![tok(TokenType::Select, "SELECT", 0)]);
    }

    #[test]
    fn keyword_select_lowercase() {
        assert_eq!(lex("select"), vec![tok(TokenType::Select, "select", 0)]);
    }

    #[test]
    fn keyword_from() {
        assert_eq!(lex("FROM"), vec![tok(TokenType::From, "FROM", 0)]);
    }

    #[test]
    fn keyword_where() {
        assert_eq!(lex("WHERE"), vec![tok(TokenType::Where, "WHERE", 0)]);
    }

    #[test]
    fn keyword_null() {
        assert_eq!(lex("NULL"), vec![tok(TokenType::Null, "NULL", 0)]);
    }

    #[test]
    fn keyword_auto_increment() {
        assert_eq!(
            lex("AUTO_INCREMENT"),
            vec![tok(TokenType::AutoIncrement, "AUTO_INCREMENT", 0)]
        );
    }

    #[test]
    fn type_keyword_integer_alias() {
        assert_eq!(lex("INTEGER"), vec![tok(TokenType::Int, "INTEGER", 0)]);
    }

    #[test]
    fn type_keyword_bool_alias() {
        assert_eq!(lex("BOOL"), vec![tok(TokenType::Boolean, "BOOL", 0)]);
    }

    #[test]
    fn type_keyword_real_alias() {
        assert_eq!(lex("REAL"), vec![tok(TokenType::Float, "REAL", 0)]);
    }

    #[test]
    fn type_keyword_double_alias() {
        assert_eq!(lex("DOUBLE"), vec![tok(TokenType::Float, "DOUBLE", 0)]);
    }

    #[test]
    fn simple_identifier() {
        assert_eq!(lex("foo"), vec![tok(TokenType::Identifier, "foo", 0)]);
    }

    #[test]
    fn identifier_with_underscore() {
        assert_eq!(
            lex("user_id"),
            vec![tok(TokenType::Identifier, "user_id", 0)]
        );
    }

    #[test]
    fn identifier_starting_with_underscore() {
        assert_eq!(lex("_col"), vec![tok(TokenType::Identifier, "_col", 0)]);
    }

    #[test]
    fn identifier_with_trailing_digits() {
        assert_eq!(lex("col1"), vec![tok(TokenType::Identifier, "col1", 0)]);
    }

    #[test]
    fn invalid_character_returns_error() {
        assert_eq!(
            try_lex("@foo").unwrap_err(),
            LexError::InvalidCharacter {
                ch: '@',
                position: 0
            }
        );
    }

    #[test]
    fn invalid_character_mid_stream() {
        let results: Vec<_> = Lexer::new("SELECT @").collect();
        assert!(results[0].is_ok()); // SELECT
        assert!(results[1].is_err()); // @
    }

    #[test]
    fn select_star_from() {
        assert_eq!(
            lex("SELECT * FROM"),
            vec![
                tok(TokenType::Select, "SELECT", 0),
                tok(TokenType::Asterisk, "*", 7),
                tok(TokenType::From, "FROM", 9),
            ]
        );
    }

    #[test]
    fn where_clause_with_operator_and_integer() {
        assert_eq!(
            lex("WHERE age > 18"),
            vec![
                tok(TokenType::Where, "WHERE", 0),
                tok(TokenType::Identifier, "age", 6),
                tok(TokenType::Operator, ">", 10),
                tok(TokenType::Int, "18", 12),
            ]
        );
    }

    #[test]
    fn where_clause_with_string_literal() {
        assert_eq!(
            lex("WHERE name = 'Alice'"),
            vec![
                tok(TokenType::Where, "WHERE", 0),
                tok(TokenType::Identifier, "name", 6),
                tok(TokenType::Operator, "=", 11),
                tok(TokenType::String, "Alice", 13),
            ]
        );
    }

    #[test]
    fn function_call_like_sequence() {
        assert_eq!(
            lex("count(id)"),
            vec![
                tok(TokenType::Identifier, "count", 0),
                tok(TokenType::Lparen, "(", 5),
                tok(TokenType::Identifier, "id", 6),
                tok(TokenType::Rparen, ")", 8),
            ]
        );
    }

    #[test]
    fn column_list_with_commas() {
        assert_eq!(
            lex("a, b, c"),
            vec![
                tok(TokenType::Identifier, "a", 0),
                tok(TokenType::Comma, ",", 1),
                tok(TokenType::Identifier, "b", 3),
                tok(TokenType::Comma, ",", 4),
                tok(TokenType::Identifier, "c", 6),
            ]
        );
    }

    #[test]
    fn statement_ending_with_semicolon() {
        let tokens = lex("SELECT 1;");
        assert_eq!(tokens.last().unwrap(), &tok(TokenType::Semicolon, ";", 8));
    }

    #[test]
    fn iterator_stops_at_end_of_input() {
        let mut lexer = Lexer::new("42");
        assert!(lexer.next().is_some());
        assert!(lexer.next().is_none());
        assert!(lexer.next().is_none()); // stable after exhaustion
    }

    #[test]
    fn iterator_short_circuits_on_error() {
        let results: Vec<_> = Lexer::new("SELECT @bad").collect();
        assert!(results[0].is_ok()); // SELECT
        assert!(results[1].is_err()); // @
    }
}
