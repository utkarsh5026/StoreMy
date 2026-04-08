use crate::parser::token::{Token, TokenType};

// 1. The Error Enum
// We carry exactly the data needed to understand the error, no more, no less.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LexError {
    /// Lexical error: Found a character that doesn't belong in SQL.
    InvalidCharacter {
        ch: char,
        position: usize,
    },

    /// Syntax error: The parser expected one thing, but found another.
    UnexpectedToken {
        expected: String,
        found: super::token::TokenType, // Re-using our TokenType enum from earlier!
        position: usize,
    },

    /// Syntax error: The query ended prematurely.
    UnexpectedEof {
        expected: String,
    },
    UnrecognizedKeyword(String),
}

pub(super) struct Lexer {
    input: Vec<char>,
    position: usize,
    length: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Lexer {
            input: input.chars().collect(),
            position: 0,
            length: input.len(),
        }
    }

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
                let string_value = self.read_string()?;
                Ok(Some((TokenType::String, string_value, start).into()))
            }
            c if c.is_ascii_digit() => {
                let number = self.read_while(|c| c.is_ascii_digit(), true).unwrap();
                Ok(Some((TokenType::Int, number, start).into()))
            }
            c if c.is_ascii_alphabetic() || *c == '_' => {
                let ident = self
                    .read_while(|ch| ch.is_alphanumeric() || ch == '_', true)
                    .unwrap();
                let token_type = match ident.parse::<TokenType>() {
                    Ok(t) => t,
                    Err(_) => TokenType::Identifier,
                };
                Ok(Some((token_type, ident, start).into()))
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
                Ok(Some((token_type, ch.to_string(), start).into()))
            }

            _ => Err(LexError::InvalidCharacter {
                ch: *ch,
                position: start,
            }),
        }
    }

    pub fn reset(&mut self) {
        self.position = 0;
    }

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

impl Iterator for Lexer {
    type Item = Result<super::token::Token, LexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.length {
            return None;
        }

        self.next_token().transpose() // Convert Result<Option<Token>, LexError> to Option<Result<Token, LexError>>
    }
}
