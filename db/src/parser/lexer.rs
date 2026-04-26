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

use thiserror::Error;

use crate::parser::token::{Span, Token, TokenType};

/// Errors that can occur while lexing a SQL string.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum LexError {
    /// A character was found that is not part of any valid SQL token.
    #[error("invalid character '{ch}' at {span}")]
    InvalidCharacter {
        /// The offending character.
        ch: char,
        /// Byte range of the offending character in the original input.
        span: Span,
    },

    /// The lexer expected a particular token kind but found something else.
    #[error("expected {expected}, but found {found:?} at {span}")]
    UnexpectedToken {
        /// Description of what was expected (e.g. `"closing parenthesis"`).
        expected: String,
        /// The token kind that was actually found.
        found: super::token::TokenType,
        /// Byte range of the offending token.
        span: Span,
    },

    /// The input ended before a required token was found.
    #[error("unexpected end of file: expected {expected}")]
    UnexpectedEof {
        /// Description of what was expected (e.g. `"closing quote '''"`).
        expected: String,
        /// Zero-length span at the end of the input.
        span: Span,
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
    /// Set to `true` after the iterator yields a [`LexError`].  Once poisoned,
    /// the [`Iterator`] impl returns `None` to prevent infinite loops on
    /// non-advancing errors (e.g. an invalid character).
    poisoned: bool,
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
            poisoned: false,
        }
    }

    /// Builds a [`Token`] from its components and saves it as `curr_token`.
    ///
    /// Every path that produces a token (except string and operator literals)
    /// goes through here so that `backtrack` always has something to restore.
    fn create_token(&mut self, kind: TokenType, val: String, start: usize) -> Token {
        let span = Span::new(start, self.position);
        self.curr_token = Some((kind, val.clone(), span).into());
        (kind, val, span).into()
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
        if self.exhausted() {
            return Ok(None);
        }

        let start = self.position;
        let ch = self.curr_char();

        match ch {
            '=' | '<' | '>' | '!' => {
                let op = self.read_operator();
                Ok(Some(self.create_token(TokenType::Operator, op, start)))
            }

            '\'' | '"' => {
                let s = self.read_string()?;
                Ok(Some(self.create_token(TokenType::String, s, start)))
            }

            c if c.is_ascii_digit() => {
                let (num, token_type) = self.read_number();
                Ok(Some(self.create_token(token_type, num, start)))
            }

            c if c.is_ascii_alphabetic() || c == '_' => {
                let (ident, token_type) = self.read_identifier();
                Ok(Some(self.create_token(token_type, ident, start)))
            }

            ',' | ';' | '(' | ')' | '*' | '.' => {
                self.advance_pos();
                let token_type = match ch {
                    ',' => TokenType::Comma,
                    ';' => TokenType::Semicolon,
                    '(' => TokenType::Lparen,
                    ')' => TokenType::Rparen,
                    '*' => TokenType::Asterisk,
                    '.' => TokenType::Dot,
                    _ => unreachable!(),
                };
                Ok(Some(self.create_token(token_type, ch.to_string(), start)))
            }

            _ => Err(LexError::InvalidCharacter {
                ch,
                span: Span::new(start, start + ch.len_utf8()),
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
                self.position = tok.span.start;
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
        let first = self.take_char();

        if let Some(&next) = self.input.get(self.position) {
            match (first, next) {
                ('<' | '>' | '!', '=') | ('<', '>') => {
                    self.advance_pos();
                    format!("{first}{next}")
                }
                _ => first.to_string(),
            }
        } else {
            first.to_string()
        }
    }

    /// Consumes a run of alphanumeric characters and underscores.
    ///
    /// If the text matches a known keyword, returns that [`TokenType`];
    /// otherwise returns [`TokenType::Identifier`].
    fn read_identifier(&mut self) -> (String, TokenType) {
        let ident = self
            .read_while(|ch| ch.is_alphanumeric() || ch == '_', true)
            .unwrap();
        let token_type = match ident.parse::<TokenType>() {
            Ok(t) => t,
            Err(_) => TokenType::Identifier,
        };
        (ident, token_type)
    }

    /// Consumes a decimal integer, or a floating literal when a dot is followed by a digit.
    ///
    /// A lone `.` after digits is not consumed as part of the number (e.g. `12.` stops at
    /// [`TokenType::Int`] `"12"`).  Sequences like `3.14` become [`TokenType::FloatLit`].
    fn read_number(&mut self) -> (String, TokenType) {
        let mut num = self.read_while(|c| c.is_ascii_digit(), true).unwrap();
        let has_fraction = !self.exhausted()
            && self.curr_char() == '.'
            && self
                .input
                .get(self.position + 1)
                .is_some_and(char::is_ascii_digit);

        if has_fraction {
            num.push('.');
            self.advance_pos();
            let frac = self.read_while(|c| c.is_ascii_digit(), true).unwrap();
            num.push_str(&frac);
            (num, TokenType::FloatLit)
        } else {
            (num, TokenType::Int)
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
        let quote_char = self.take_char();
        let mut value = String::new();

        while !self.exhausted() {
            let curr = self.take_char();

            if curr == quote_char {
                return Ok(value);
            }
            value.push(curr);
        }

        Err(LexError::UnexpectedEof {
            expected: format!("closing quote '{quote_char}'"),
            span: Span::point(self.length),
        })
    }

    /// Advances `position` as long as `condition` holds for the current character.
    ///
    /// When `collect` is `true` the consumed characters are gathered into a
    /// `String` and returned as `Some(String)`.  When `collect` is `false` the
    /// characters are discarded and `None` is returned (useful for skipping
    /// whitespace without allocating).
    fn read_while<F>(&mut self, condition: F, collect: bool) -> Option<String>
    where
        F: Fn(char) -> bool,
    {
        let mut result = String::new();
        while !self.exhausted() && condition(self.curr_char()) {
            let ch = self.take_char();
            if collect {
                result.push(ch);
            }
        }

        if collect { Some(result) } else { None }
    }

    /// Returns the current character without advancing the position.
    /// # Panics
    /// Panics if the position is out of bounds.
    #[inline]
    fn curr_char(&self) -> char {
        *self.input.get(self.position).unwrap_or_else(|| {
            panic!("position out of bounds: {}", self.position);
        })
    }

    /// Advances the position by 1.
    /// # Panics
    /// Panics if the position is out of bounds.
    #[inline]
    fn advance_pos(&mut self) {
        self.position = self.position.checked_add(1).unwrap_or_else(|| {
            panic!("position out of bounds: {}", self.position);
        });
    }

    /// Returns the current character and advances the position by one.
    ///
    /// # Panics
    /// Panics if the position is out of bounds when attempting to read or advance.
    #[inline]
    fn take_char(&mut self) -> char {
        let ch = self.curr_char();
        self.advance_pos();
        ch
    }

    /// Returns `true` if the lexer has consumed all input or is in a poisoned (error) state.
    ///
    /// # Returns
    /// * `true` if `self.position` is greater than or equal to the total input `self.length`, or if
    ///   `self.poisoned` is `true` due to a lexing error.
    #[inline]
    fn exhausted(&self) -> bool {
        self.position >= self.length || self.poisoned
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
        if self.exhausted() {
            return None;
        }

        let item = self.next_token().transpose();
        if matches!(item, Some(Err(_))) {
            self.poisoned = true;
        }
        item
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
        // For most tokens the span length equals value.len() — true for
        // keywords, identifiers, integers, operators, and punctuation. String
        // literals are the exception (the span includes surrounding quotes);
        // tests that exercise strings either don't assert the span or build
        // the token explicitly.
        let end = position + value.len();
        Token {
            kind,
            value: value.to_string(),
            span: Span::new(position, end),
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
    fn simple_float_literal() {
        assert_eq!(lex("3.14"), vec![tok(TokenType::FloatLit, "3.14", 0)]);
    }

    #[test]
    fn float_literal_zero_fraction() {
        assert_eq!(lex("0.5"), vec![tok(TokenType::FloatLit, "0.5", 0)]);
    }

    #[test]
    fn trailing_dot_is_not_consumed_as_float() {
        // "1." should lex as Int("1") first; the dangling "." is not part
        // of the number.  We only assert on the first token — what comes
        // after depends on how "." is handled elsewhere.
        let mut lexer = Lexer::new("1.");
        let first = lexer.next().unwrap().unwrap();
        assert_eq!(first, tok(TokenType::Int, "1", 0));
    }

    #[test]
    fn digit_dot_alpha_does_not_lex_as_float() {
        // "1.foo" — the "1" must not swallow the "." because no digit follows.
        let mut lexer = Lexer::new("1.foo");
        let first = lexer.next().unwrap().unwrap();
        assert_eq!(first, tok(TokenType::Int, "1", 0));
    }

    #[test]
    fn float_literal_then_comma() {
        assert_eq!(
            lex("2.5,"),
            vec![
                tok(TokenType::FloatLit, "2.5", 0),
                tok(TokenType::Comma, ",", 3),
            ]
        );
    }

    /// Build a String-literal token whose span covers the surrounding quotes,
    /// while `value` is the unquoted contents.
    fn str_tok(value: &str, start: usize) -> Token {
        Token {
            kind: TokenType::String,
            value: value.to_string(),
            span: Span::new(start, start + value.len() + 2),
        }
    }

    #[test]
    fn single_quoted_string() {
        assert_eq!(lex("'hello'"), vec![str_tok("hello", 0)]);
    }

    #[test]
    fn double_quoted_string() {
        assert_eq!(lex("\"world\""), vec![str_tok("world", 0)]);
    }

    #[test]
    fn string_with_spaces_inside() {
        assert_eq!(lex("'hello world'"), vec![str_tok("hello world", 0)]);
    }

    #[test]
    fn empty_string_literal() {
        assert_eq!(lex("''"), vec![str_tok("", 0)]);
    }

    #[test]
    fn unterminated_single_quoted_string() {
        assert_eq!(
            try_lex("'oops").unwrap_err(),
            LexError::UnexpectedEof {
                expected: "closing quote '''".to_string(),
                span: Span::point("'oops".len()),
            }
        );
    }

    #[test]
    fn unterminated_double_quoted_string() {
        assert_eq!(
            try_lex("\"oops").unwrap_err(),
            LexError::UnexpectedEof {
                expected: "closing quote '\"'".to_string(),
                span: Span::point("\"oops".len()),
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
                span: Span::new(0, 1),
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
                str_tok("Alice", 13),
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
