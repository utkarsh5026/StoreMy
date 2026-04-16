//! Tokens produced by the SQL lexer.
//!
//! A [`Token`] is the smallest meaningful unit of SQL text — a keyword, literal,
//! punctuation mark, or identifier. The lexer scans raw input and emits a stream
//! of tokens; the parser then consumes that stream to build an AST.
//!
//! Two types are defined here:
//!
//! - [`TokenType`] — a tag describing what category a token belongs to.
//! - [`Token`] — an owned token carrying its type, raw text, and source position.
//!
//! Several conversion traits are implemented so tokens can be cheaply turned into
//! higher-level values:
//!
//! - [`TryFrom<Token>`] for [`crate::Type`] — interprets a type-keyword token as a
//!   column data type.
//! - [`TryFrom<Token>`] for [`crate::Value`] — interprets a literal token (`INT`,
//!   `STRING`, `NULL`, or a boolean identifier) as a runtime value.
//! - [`std::str::FromStr`] for [`TokenType`] — maps an uppercase keyword string to
//!   the corresponding variant (used by the lexer's identifier scanner).

use std::fmt;

use crate::Type;
use crate::Value;

/// The category of a SQL token.
///
/// Each variant corresponds to either a SQL keyword, a literal kind, a
/// punctuation character, or a special sentinel (`Invalid`, `Eof`).
///
/// Variants for individual keywords (e.g. `Select`, `Distinct`) are not
/// individually documented here; their meaning matches the standard SQL keyword
/// of the same name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenType {
    Create,
    Table,
    Drop,
    If,
    Not,
    Exists,
    Primary,
    Key,
    Default,
    Null,
    AutoIncrement,
    Select,
    Distinct,
    From,
    Where,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    On,
    Group,
    Order,
    By,
    Limit,
    Offset,
    Index,
    Using,
    Hash,
    Btree,
    Union,
    Intersect,
    Except,
    All,

    Asc,
    Desc,
    And,
    Or,

    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,

    Begin,
    Commit,
    Rollback,
    Transaction,

    Explain,
    Analyze,
    Format,

    Show,
    Indexes,

    Int,
    Varchar,
    Text,
    Boolean,
    Float,
    Operator,
    String,
    Identifier,
    Comma,
    Semicolon,
    Lparen,
    Rparen,
    Asterisk,
    Invalid,
    Eof,
}

// We use the `Display` trait instead of a runtime HashMap.
// This compiles down to a highly optimized jump table, requiring zero heap allocations.
impl fmt::Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TokenType::Create => "CREATE",
            TokenType::Table => "TABLE",
            TokenType::Drop => "DROP",
            TokenType::If => "IF",
            TokenType::Not => "NOT",
            TokenType::Exists => "EXISTS",
            TokenType::Primary => "PRIMARY",
            TokenType::Key => "KEY",
            TokenType::Default => "DEFAULT",
            TokenType::Null => "NULL",
            TokenType::AutoIncrement => "AUTO_INCREMENT",
            TokenType::Select => "SELECT",
            TokenType::Distinct => "DISTINCT",
            TokenType::From => "FROM",
            TokenType::Where => "WHERE",
            TokenType::Join => "JOIN",
            TokenType::Inner => "INNER",
            TokenType::Left => "LEFT",
            TokenType::Right => "RIGHT",
            TokenType::Outer => "OUTER",
            TokenType::On => "ON",
            TokenType::Group => "GROUP",
            TokenType::Order => "ORDER",
            TokenType::By => "BY",
            TokenType::Limit => "LIMIT",
            TokenType::Offset => "OFFSET",
            TokenType::Index => "INDEX",
            TokenType::Using => "USING",
            TokenType::Hash => "HASH",
            TokenType::Btree => "BTREE",
            TokenType::Union => "UNION",
            TokenType::Intersect => "INTERSECT",
            TokenType::Except => "EXCEPT",
            TokenType::All => "ALL",
            TokenType::Asc => "ASC",
            TokenType::Desc => "DESC",
            TokenType::And => "AND",
            TokenType::Or => "OR",
            TokenType::Insert => "INSERT",
            TokenType::Into => "INTO",
            TokenType::Values => "VALUES",
            TokenType::Update => "UPDATE",
            TokenType::Set => "SET",
            TokenType::Delete => "DELETE",
            TokenType::Begin => "BEGIN",
            TokenType::Commit => "COMMIT",
            TokenType::Rollback => "ROLLBACK",
            TokenType::Transaction => "TRANSACTION",
            TokenType::Explain => "EXPLAIN",
            TokenType::Analyze => "ANALYZE",
            TokenType::Format => "FORMAT",
            TokenType::Show => "SHOW",
            TokenType::Indexes => "INDEXES",
            TokenType::Int => "INT",
            TokenType::Varchar => "VARCHAR",
            TokenType::Text => "TEXT",
            TokenType::Boolean => "BOOLEAN",
            TokenType::Float => "FLOAT",
            TokenType::Operator => "OPERATOR",
            TokenType::String => "STRING",
            TokenType::Identifier => "IDENTIFIER",
            TokenType::Comma => "COMMA",
            TokenType::Semicolon => "SEMICOLON",
            TokenType::Lparen => "LPAREN",
            TokenType::Rparen => "RPAREN",
            TokenType::Asterisk => "ASTERISK",
            TokenType::Invalid => "INVALID",
            TokenType::Eof => "EOF",
        };
        write!(f, "{s}")
    }
}

/// A single token produced by the lexer.
///
/// Each token records what kind it is, the exact slice of source text that
/// produced it, and the byte offset where it starts in the original input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// The category of this token.
    pub kind: TokenType,
    /// The raw source text of this token (e.g. `"SELECT"`, `"42"`, `"'hello'"`).
    pub value: String,
    /// Byte offset of the first character of this token in the original input.
    pub position: usize,
}

/// Formats the token as `KIND("value") at position N`.
impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({:?}) at position {}",
            self.kind, self.value, self.position
        )
    }
}

impl Token {
    /// Returns `true` if this token's kind equals `kind`.
    pub fn is(&self, kind: TokenType) -> bool {
        kind == self.kind
    }

    /// Returns `true` if this token's kind does not equal `kind`.
    pub fn is_not(&self, kind: TokenType) -> bool {
        !self.is(kind)
    }
}

/// Constructs a [`Token`] from a `(kind, value, position)` tuple.
impl From<(TokenType, String, usize)> for Token {
    fn from(tuple: (TokenType, String, usize)) -> Self {
        Token {
            kind: tuple.0,
            value: tuple.1,
            position: tuple.2,
        }
    }
}

/// Clones the raw source text out of a token reference as an owned `String`.
// Teach String how to be created FROM a reference to a Token
impl From<&Token> for String {
    fn from(token: &Token) -> Self {
        token.value.clone()
    }
}

/// Interprets a type-keyword token as a column [`Type`].
///
/// Only tokens whose kind is a SQL type keyword are accepted:
///
/// | `TokenType`            | `Type`          |
/// |------------------------|-----------------|
/// | `Int`                  | `Type::Int64`   |
/// | `Varchar` \| `Text`    | `Type::String`  |
/// | `Boolean`              | `Type::Bool`    |
/// | `Float`                | `Type::Float64` |
///
/// # Errors
///
/// Returns an error string for any other token kind.
impl TryFrom<Token> for Type {
    type Error = String;
    fn try_from(value: Token) -> Result<Self, Self::Error> {
        match value.kind {
            TokenType::Int => Ok(Type::Int64),
            TokenType::Varchar | TokenType::Text => Ok(Type::String),
            TokenType::Boolean => Ok(Type::Bool),
            TokenType::Float => Ok(Type::Float64),
            _ => Err(format!("unknown data type: {0}", value.kind)),
        }
    }
}

/// Interprets a literal token as a runtime [`Value`].
///
/// The following token kinds are recognized:
///
/// | `TokenType`  | Produces                                              |
/// |--------------|-------------------------------------------------------|
/// | `Int`        | `Value::Int64` — the value string parsed as `i64`    |
/// | `String`     | `Value::String` — the raw (unquoted) value string    |
/// | `Null`       | `Value::Null`                                         |
/// | `Identifier` | `Value::Bool(true/false)` when the text is `"true"` or `"false"` (case-insensitive) |
///
/// # Errors
///
/// Returns an error string when:
/// - An `Int` token's value cannot be parsed as `i64`.
/// - An `Identifier` token's value is not `"true"` or `"false"`.
/// - Any other token kind is passed.
impl TryFrom<Token> for Value {
    type Error = String; // or a proper error type

    fn try_from(token: Token) -> Result<Self, Self::Error> {
        match token.kind {
            TokenType::Int => token
                .value
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|e| format!("invalid integer literal '{}': {e}", token.value)),

            TokenType::String => Ok(Value::String(token.value)),

            TokenType::Null => Ok(Value::Null),

            TokenType::Identifier => match token.value.to_ascii_lowercase().as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => Err(format!(
                    "identifier '{}' is not a literal value",
                    token.value
                )),
            },

            _ => Err(format!(
                "token {} cannot be converted to a Value",
                token.kind
            )),
        }
    }
}

/// Parses an uppercase SQL keyword string into a [`TokenType`].
///
/// The input is converted to uppercase before matching, so `"select"` and
/// `"SELECT"` both produce [`TokenType::Select`].
///
/// # Errors
///
/// Returns `Err("Cannot find any keyword related to this")` for any string that
/// does not match a known keyword.
impl std::str::FromStr for TokenType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper_s = s.to_ascii_uppercase();

        match upper_s.as_str() {
            "SELECT" => Ok(TokenType::Select),
            "DISTINCT" => Ok(TokenType::Distinct),
            "FROM" => Ok(TokenType::From),
            "WHERE" => Ok(TokenType::Where),
            "JOIN" => Ok(TokenType::Join),
            "INNER" => Ok(TokenType::Inner),
            "LEFT" => Ok(TokenType::Left),
            "RIGHT" => Ok(TokenType::Right),
            "OUTER" => Ok(TokenType::Outer),
            "ON" => Ok(TokenType::On),
            "GROUP" => Ok(TokenType::Group),
            "BY" => Ok(TokenType::By),
            "ORDER" => Ok(TokenType::Order),
            "LIMIT" => Ok(TokenType::Limit),
            "OFFSET" => Ok(TokenType::Offset),
            "ASC" => Ok(TokenType::Asc),
            "DESC" => Ok(TokenType::Desc),
            "AND" => Ok(TokenType::And),
            "OR" => Ok(TokenType::Or),
            "CREATE" => Ok(TokenType::Create),
            "TABLE" => Ok(TokenType::Table),
            "DROP" => Ok(TokenType::Drop),
            "IF" => Ok(TokenType::If),
            "NOT" => Ok(TokenType::Not),
            "EXISTS" => Ok(TokenType::Exists),
            "PRIMARY" => Ok(TokenType::Primary),
            "KEY" => Ok(TokenType::Key),
            "INDEX" => Ok(TokenType::Index),
            "USING" => Ok(TokenType::Using),
            "HASH" => Ok(TokenType::Hash),
            "BTREE" => Ok(TokenType::Btree),
            "UNION" => Ok(TokenType::Union),
            "INTERSECT" => Ok(TokenType::Intersect),
            "EXCEPT" => Ok(TokenType::Except),
            "ALL" => Ok(TokenType::All),
            "DEFAULT" => Ok(TokenType::Default),
            "NULL" => Ok(TokenType::Null),
            "AUTO_INCREMENT" => Ok(TokenType::AutoIncrement),
            "INSERT" => Ok(TokenType::Insert),
            "INTO" => Ok(TokenType::Into),
            "VALUES" => Ok(TokenType::Values),
            "UPDATE" => Ok(TokenType::Update),
            "SET" => Ok(TokenType::Set),
            "DELETE" => Ok(TokenType::Delete),
            "BEGIN" => Ok(TokenType::Begin),
            "COMMIT" => Ok(TokenType::Commit),
            "ROLLBACK" => Ok(TokenType::Rollback),
            "TRANSACTION" => Ok(TokenType::Transaction),
            "EXPLAIN" => Ok(TokenType::Explain),
            "ANALYZE" => Ok(TokenType::Analyze),
            "FORMAT" => Ok(TokenType::Format),
            "SHOW" => Ok(TokenType::Show),
            "INDEXES" => Ok(TokenType::Indexes),

            "INT" | "INTEGER" => Ok(TokenType::Int),
            "VARCHAR" | "STRING" => Ok(TokenType::Varchar),
            "TEXT" => Ok(TokenType::Text),
            "BOOLEAN" | "BOOL" => Ok(TokenType::Boolean),
            "FLOAT" | "REAL" | "DOUBLE" => Ok(TokenType::Float),

            _ => Err("Cannot find any keyword related to this"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn make_token(kind: TokenType, value: &str) -> Token {
        Token {
            kind,
            value: value.to_string(),
            position: 0,
        }
    }

    // From<(TokenType, String, usize)> maps fields correctly
    #[test]
    fn test_token_from_tuple_fields() {
        let token = Token::from((TokenType::Select, "SELECT".to_string(), 42));
        assert_eq!(token.kind, TokenType::Select);
        assert_eq!(token.value, "SELECT");
        assert_eq!(token.position, 42);
    }

    // From<&Token> for String clones the value field
    #[test]
    fn test_string_from_token_ref() {
        let token = make_token(TokenType::Identifier, "my_table");
        let s = String::from(&token);
        assert_eq!(s, "my_table");
    }

    // is() returns true when kind matches
    #[test]
    fn test_token_is_matching_kind() {
        let token = make_token(TokenType::Select, "SELECT");
        assert!(token.is(TokenType::Select));
    }

    // is() returns false when kind differs
    #[test]
    fn test_token_is_wrong_kind() {
        let token = make_token(TokenType::Select, "SELECT");
        assert!(!token.is(TokenType::From));
    }

    // is_not() is the inverse of is()
    #[test]
    fn test_token_is_not() {
        let token = make_token(TokenType::Comma, ",");
        assert!(token.is_not(TokenType::Semicolon));
        assert!(!token.is_not(TokenType::Comma));
    }

    // Display format is KIND("value") at position N
    #[test]
    fn test_token_display_format() {
        let token = Token {
            kind: TokenType::Identifier,
            value: "foo".to_string(),
            position: 7,
        };
        assert_eq!(format!("{token}"), r#"IDENTIFIER("foo") at position 7"#);
    }

    // Every keyword variant stringifies to its SQL spelling
    #[test]
    fn test_token_type_display_keywords() {
        let cases = [
            (TokenType::Select, "SELECT"),
            (TokenType::Distinct, "DISTINCT"),
            (TokenType::From, "FROM"),
            (TokenType::Where, "WHERE"),
            (TokenType::Join, "JOIN"),
            (TokenType::Inner, "INNER"),
            (TokenType::Left, "LEFT"),
            (TokenType::Right, "RIGHT"),
            (TokenType::Outer, "OUTER"),
            (TokenType::On, "ON"),
            (TokenType::Group, "GROUP"),
            (TokenType::Order, "ORDER"),
            (TokenType::By, "BY"),
            (TokenType::Limit, "LIMIT"),
            (TokenType::Offset, "OFFSET"),
            (TokenType::Asc, "ASC"),
            (TokenType::Desc, "DESC"),
            (TokenType::And, "AND"),
            (TokenType::Or, "OR"),
            (TokenType::Create, "CREATE"),
            (TokenType::Table, "TABLE"),
            (TokenType::Drop, "DROP"),
            (TokenType::If, "IF"),
            (TokenType::Not, "NOT"),
            (TokenType::Exists, "EXISTS"),
            (TokenType::Primary, "PRIMARY"),
            (TokenType::Key, "KEY"),
            (TokenType::Default, "DEFAULT"),
            (TokenType::Null, "NULL"),
            (TokenType::AutoIncrement, "AUTO_INCREMENT"),
            (TokenType::Insert, "INSERT"),
            (TokenType::Into, "INTO"),
            (TokenType::Values, "VALUES"),
            (TokenType::Update, "UPDATE"),
            (TokenType::Set, "SET"),
            (TokenType::Delete, "DELETE"),
            (TokenType::Begin, "BEGIN"),
            (TokenType::Commit, "COMMIT"),
            (TokenType::Rollback, "ROLLBACK"),
            (TokenType::Transaction, "TRANSACTION"),
            (TokenType::Explain, "EXPLAIN"),
            (TokenType::Analyze, "ANALYZE"),
            (TokenType::Format, "FORMAT"),
            (TokenType::Show, "SHOW"),
            (TokenType::Indexes, "INDEXES"),
            (TokenType::Index, "INDEX"),
            (TokenType::Using, "USING"),
            (TokenType::Hash, "HASH"),
            (TokenType::Btree, "BTREE"),
            (TokenType::Union, "UNION"),
            (TokenType::Intersect, "INTERSECT"),
            (TokenType::Except, "EXCEPT"),
            (TokenType::All, "ALL"),
        ];
        for (kind, expected) in cases {
            assert_eq!(kind.to_string(), expected, "failed for {kind:?}");
        }
    }

    // Type and literal sentinel variants
    #[test]
    fn test_token_type_display_type_and_sentinel_variants() {
        assert_eq!(TokenType::Int.to_string(), "INT");
        assert_eq!(TokenType::Varchar.to_string(), "VARCHAR");
        assert_eq!(TokenType::Text.to_string(), "TEXT");
        assert_eq!(TokenType::Boolean.to_string(), "BOOLEAN");
        assert_eq!(TokenType::Float.to_string(), "FLOAT");
        assert_eq!(TokenType::Operator.to_string(), "OPERATOR");
        assert_eq!(TokenType::String.to_string(), "STRING");
        assert_eq!(TokenType::Identifier.to_string(), "IDENTIFIER");
        assert_eq!(TokenType::Comma.to_string(), "COMMA");
        assert_eq!(TokenType::Semicolon.to_string(), "SEMICOLON");
        assert_eq!(TokenType::Lparen.to_string(), "LPAREN");
        assert_eq!(TokenType::Rparen.to_string(), "RPAREN");
        assert_eq!(TokenType::Asterisk.to_string(), "ASTERISK");
        assert_eq!(TokenType::Invalid.to_string(), "INVALID");
        assert_eq!(TokenType::Eof.to_string(), "EOF");
    }

    // Canonical uppercase keyword strings parse correctly
    #[test]
    fn test_from_str_canonical_keywords() {
        let cases = [
            ("SELECT", TokenType::Select),
            ("FROM", TokenType::From),
            ("WHERE", TokenType::Where),
            ("CREATE", TokenType::Create),
            ("INSERT", TokenType::Insert),
            ("UPDATE", TokenType::Update),
            ("DELETE", TokenType::Delete),
            ("BEGIN", TokenType::Begin),
            ("COMMIT", TokenType::Commit),
            ("ROLLBACK", TokenType::Rollback),
            ("EXPLAIN", TokenType::Explain),
            ("SHOW", TokenType::Show),
            ("INDEXES", TokenType::Indexes),
        ];
        for (s, expected) in cases {
            assert_eq!(TokenType::from_str(s).unwrap(), expected, "failed for {s}");
        }
    }

    // Lowercase input is normalized to uppercase before matching
    #[test]
    fn test_from_str_case_insensitive() {
        assert_eq!(TokenType::from_str("select").unwrap(), TokenType::Select);
        assert_eq!(TokenType::from_str("Select").unwrap(), TokenType::Select);
        assert_eq!(TokenType::from_str("fRoM").unwrap(), TokenType::From);
    }

    // Keyword aliases map to the correct canonical variant
    #[test]
    fn test_from_str_aliases() {
        assert_eq!(TokenType::from_str("INTEGER").unwrap(), TokenType::Int);
        assert_eq!(TokenType::from_str("BOOL").unwrap(), TokenType::Boolean);
        assert_eq!(TokenType::from_str("REAL").unwrap(), TokenType::Float);
        assert_eq!(TokenType::from_str("DOUBLE").unwrap(), TokenType::Float);
        assert_eq!(TokenType::from_str("VARCHAR").unwrap(), TokenType::Varchar);
        assert_eq!(TokenType::from_str("STRING").unwrap(), TokenType::Varchar);
    }

    // Unrecognized strings return Err
    #[test]
    fn test_from_str_unknown_keyword() {
        assert!(TokenType::from_str("FOOBAR").is_err());
        assert!(TokenType::from_str("").is_err());
        assert!(TokenType::from_str("123").is_err());
    }

    // Every token type whose Display spelling is also a valid FromStr input
    // round-trips back to itself.
    #[test]
    fn test_display_from_str_round_trip() {
        let keyword_types = [
            TokenType::Select,
            TokenType::Distinct,
            TokenType::From,
            TokenType::Where,
            TokenType::Join,
            TokenType::Inner,
            TokenType::Left,
            TokenType::Right,
            TokenType::Outer,
            TokenType::On,
            TokenType::Group,
            TokenType::Order,
            TokenType::By,
            TokenType::Limit,
            TokenType::Offset,
            TokenType::Asc,
            TokenType::Desc,
            TokenType::And,
            TokenType::Or,
            TokenType::Create,
            TokenType::Table,
            TokenType::Drop,
            TokenType::If,
            TokenType::Not,
            TokenType::Exists,
            TokenType::Primary,
            TokenType::Key,
            TokenType::Default,
            TokenType::Null,
            TokenType::AutoIncrement,
            TokenType::Insert,
            TokenType::Into,
            TokenType::Values,
            TokenType::Update,
            TokenType::Set,
            TokenType::Delete,
            TokenType::Begin,
            TokenType::Commit,
            TokenType::Rollback,
            TokenType::Transaction,
            TokenType::Explain,
            TokenType::Analyze,
            TokenType::Format,
            TokenType::Show,
            TokenType::Indexes,
            TokenType::Index,
            TokenType::Using,
            TokenType::Hash,
            TokenType::Btree,
            TokenType::Union,
            TokenType::Intersect,
            TokenType::Except,
            TokenType::All,
            TokenType::Int,
            TokenType::Text,
            TokenType::Boolean,
            TokenType::Float,
        ];
        for tt in keyword_types {
            let displayed = tt.to_string();
            let parsed = TokenType::from_str(&displayed).unwrap_or_else(|_| {
                panic!("round-trip failed for {tt:?}: Display produced {displayed:?}")
            });
            assert_eq!(parsed, tt, "round-trip mismatch for {tt:?}");
        }
    }

    // Each SQL type keyword token converts to the correct Type variant
    #[test]
    fn test_try_from_token_for_type_happy() {
        assert_eq!(
            Type::try_from(make_token(TokenType::Int, "INT")).unwrap(),
            Type::Int64
        );
        assert_eq!(
            Type::try_from(make_token(TokenType::Varchar, "VARCHAR")).unwrap(),
            Type::String
        );
        assert_eq!(
            Type::try_from(make_token(TokenType::Text, "TEXT")).unwrap(),
            Type::String
        );
        assert_eq!(
            Type::try_from(make_token(TokenType::Boolean, "BOOLEAN")).unwrap(),
            Type::Bool
        );
        assert_eq!(
            Type::try_from(make_token(TokenType::Float, "FLOAT")).unwrap(),
            Type::Float64
        );
    }

    // Non-type tokens produce an error containing the token kind name
    #[test]
    fn test_try_from_token_for_type_error_on_keyword() {
        let result = Type::try_from(make_token(TokenType::Select, "SELECT"));
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("SELECT"),
            "error message should name the bad kind: {msg}"
        );
    }

    #[test]
    fn test_try_from_token_for_type_error_on_identifier() {
        let result = Type::try_from(make_token(TokenType::Identifier, "mytype"));
        assert!(result.is_err());
    }

    #[test]
    fn test_try_from_token_for_type_error_on_eof() {
        let result = Type::try_from(make_token(TokenType::Eof, ""));
        assert!(result.is_err());
    }

    // Int token with a valid i64 string produces Value::Int64
    #[test]
    fn test_try_from_token_for_value_int_positive() {
        let result = Value::try_from(make_token(TokenType::Int, "42")).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_try_from_token_for_value_int_negative() {
        let result = Value::try_from(make_token(TokenType::Int, "-7")).unwrap();
        assert_eq!(result, Value::Int64(-7));
    }

    #[test]
    fn test_try_from_token_for_value_int_zero() {
        let result = Value::try_from(make_token(TokenType::Int, "0")).unwrap();
        assert_eq!(result, Value::Int64(0));
    }

    // i64::MAX and i64::MIN are valid
    #[test]
    fn test_try_from_token_for_value_int_boundary() {
        let max = i64::MAX.to_string();
        let min = i64::MIN.to_string();
        assert_eq!(
            Value::try_from(make_token(TokenType::Int, &max)).unwrap(),
            Value::Int64(i64::MAX)
        );
        assert_eq!(
            Value::try_from(make_token(TokenType::Int, &min)).unwrap(),
            Value::Int64(i64::MIN)
        );
    }

    // String token produces Value::String with the raw value
    #[test]
    fn test_try_from_token_for_value_string() {
        let result = Value::try_from(make_token(TokenType::String, "hello world")).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    // Empty string is a valid String token
    #[test]
    fn test_try_from_token_for_value_string_empty() {
        let result = Value::try_from(make_token(TokenType::String, "")).unwrap();
        assert_eq!(result, Value::String(String::new()));
    }

    // Null token always produces Value::Null regardless of value text
    #[test]
    fn test_try_from_token_for_value_null() {
        let result = Value::try_from(make_token(TokenType::Null, "NULL")).unwrap();
        assert_eq!(result, Value::Null);
    }

    // Identifier "true"/"false" are case-insensitive
    #[test]
    fn test_try_from_token_for_value_bool_lowercase() {
        let t = Value::try_from(make_token(TokenType::Identifier, "true")).unwrap();
        let f = Value::try_from(make_token(TokenType::Identifier, "false")).unwrap();
        assert_eq!(t, Value::Bool(true));
        assert_eq!(f, Value::Bool(false));
    }

    #[test]
    fn test_try_from_token_for_value_bool_uppercase() {
        let t = Value::try_from(make_token(TokenType::Identifier, "TRUE")).unwrap();
        let f = Value::try_from(make_token(TokenType::Identifier, "FALSE")).unwrap();
        assert_eq!(t, Value::Bool(true));
        assert_eq!(f, Value::Bool(false));
    }

    #[test]
    fn test_try_from_token_for_value_bool_mixed_case() {
        let t = Value::try_from(make_token(TokenType::Identifier, "True")).unwrap();
        let f = Value::try_from(make_token(TokenType::Identifier, "FaLsE")).unwrap();
        assert_eq!(t, Value::Bool(true));
        assert_eq!(f, Value::Bool(false));
    }

    // Int token with non-numeric text produces a parse error
    #[test]
    fn test_try_from_token_for_value_int_bad_string() {
        let result = Value::try_from(make_token(TokenType::Int, "not_a_number"));
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("not_a_number"),
            "error message should echo the bad value: {msg}"
        );
    }

    // Int token with value exceeding i64 range produces an error
    #[test]
    fn test_try_from_token_for_value_int_overflow() {
        let overflow = "99999999999999999999999999999";
        let result = Value::try_from(make_token(TokenType::Int, overflow));
        assert!(result.is_err());
    }

    // Identifier that is neither "true" nor "false" produces an error
    #[test]
    fn test_try_from_token_for_value_identifier_non_bool() {
        let result = Value::try_from(make_token(TokenType::Identifier, "my_column"));
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("my_column"),
            "error message should name the identifier: {msg}"
        );
    }

    // A keyword token (Select, From, …) is not a valid Value
    #[test]
    fn test_try_from_token_for_value_keyword_is_error() {
        let result = Value::try_from(make_token(TokenType::Select, "SELECT"));
        assert!(result.is_err());
    }

    // Eof token is not a valid Value
    #[test]
    fn test_try_from_token_for_value_eof_is_error() {
        let result = Value::try_from(make_token(TokenType::Eof, ""));
        assert!(result.is_err());
    }

    // Operator token is not a valid Value
    #[test]
    fn test_try_from_token_for_value_operator_is_error() {
        let result = Value::try_from(make_token(TokenType::Operator, "="));
        assert!(result.is_err());
    }

    // PartialEq and Copy are derived; verify a copied value compares equal
    #[test]
    fn test_token_type_copy_and_eq() {
        let a = TokenType::Join;
        let b = a; // Copy
        assert_eq!(a, b);
    }

    // Hash is derived; verify two equal values hash the same way via HashMap
    #[test]
    fn test_token_type_hash_in_map() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(TokenType::Select, "select");
        assert_eq!(map[&TokenType::Select], "select");
        assert!(!map.contains_key(&TokenType::From));
    }
}
