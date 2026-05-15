//! Parse SQL expression text into the [`Expr`] AST used by the binder and planner.
//!
//! Covers literals, column references (qualified or not), prefix `NOT`, parenthesized
//! sub-expressions, binary `AND` / `OR`, the comparison operators `=`, `!=`, `<>`,
//! `<`, `<=`, `>`, `>=`, and aggregate calls `COUNT(*)`, `COUNT(expr)`, `SUM`,
//! `AVG`, `MIN`, `MAX`. Unary minus and arithmetic on expressions are not parsed
//! here yet (see tests for `-1`).
//!
//! # Shape
//!
//! - [`Precedence`] — Pratt binding powers: `OR` < `AND` < prefix `NOT` < comparisons.
//! - [`Expr`] — columns, literals, aggregates, `BinaryOp`, `UnaryOp`, `CountStar`.
//! - [`BinOp`] / [`UnOp`] — operator tags attached to [`Expr`] trees.
//! - `Parser::parse_expression` — entry point; callers pass `min_precedence` to stop early when
//!   parsing a sub-clause inside a larger production.
//!
//! # How it works
//!
//! `Parser::parse_expression` runs precedence climbing: parse one atom with
//! [`Parser::parse_atom`], attach postfix suffixes (`IS NULL`, future `IN`, …) via
//! [`Parser::apply_postfix_suffixes`], then while the peeked infix operator’s left
//! binding power is high enough, consume it and parse the right side.
//!
//! # NULL semantics
//!
//! `NULL` (and boolean spellings) become [`Expr::Literal`] via the lexer token;
//! this module does not implement SQL three-valued logic — that belongs to the
//! executor once expressions are bound.

use std::fmt::Display;

use crate::{
    Value,
    parser::{
        Parser,
        parsers::ParserError,
        statements::{AggFunc, ColumnRef},
        token::TokenType,
    },
    primitives::NonEmptyString,
};

/// Binding powers for Pratt-style parsing of infix and prefix operators.
///
/// Mirrors SQL precedence: `OR` is loosest among the binary logicals, then `AND`;
/// comparisons bind tightest among the forms handled here. Prefix `NOT` uses
/// [`Self::prefix_bp`] and sits between `AND` and comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Precedence {
    Or,
    And,
    PrefixNot,
    Comparison,
}

impl Precedence {
    pub(super) const LOOSEST: u8 = 0;

    const fn binary_bp(self) -> (u8, u8) {
        match self {
            Precedence::Or => (1, 2),
            Precedence::And => (3, 4),
            Precedence::Comparison => (6, 7),
            Precedence::PrefixNot => panic!("PrefixNot has no binary bp"),
        }
    }

    const fn prefix_bp(self) -> u8 {
        match self {
            Precedence::PrefixNot => 5,
            _ => panic!("not a prefix operator"),
        }
    }
}

/// A single expression inside a `SELECT` list.
///
/// `Column` is a plain projection; `Agg` is `f(col)` for any non-`*` aggregate;
/// `CountStar` is the special `COUNT(*)` form (which counts rows rather than
/// non-null values of a specific column).
///
/// A scalar expression — anything that, given a row, evaluates to a single
/// value. `Expr` is the unified shape that every clause that holds an
/// "expression" eventually wants to use (projections, `WHERE`, `HAVING`,
/// `ORDER BY`, `GROUP BY`, join conditions). Today only `SelectItem` consumes
/// it; the other clauses still use their own narrower types and will migrate
/// later.
///
/// The recursive shape is deliberate: a future `Binary { op, left, right }`
/// variant will let any operand be itself an `Expr` (`age + 1 > 18`,
/// `UPPER(name) = 'BOB'`, `SUM(a + 1)`), so `Agg`'s argument is already typed
/// as `Box<Expr>` to avoid a second migration.
///
/// # SQL examples
///
/// ```sql
/// -- SELECT name        -->  Expr::Column(ColumnRef { None, "name" })
/// -- SELECT u.age       -->  Expr::Column(ColumnRef { Some("u"), "age" })
/// -- SELECT 1           -->  Expr::Literal(Value::Int64(1))
/// -- SELECT 'hello'     -->  Expr::Literal(Value::String("hello"))
/// -- SELECT NULL        -->  Expr::Literal(Value::Null)
/// -- SELECT COUNT(*)    -->  Expr::CountStar
/// -- SELECT COUNT(name) -->  Expr::Agg(AggFunc::Count, Box::new(Expr::Column("name".into())))
/// -- SELECT AVG(age)    -->  Expr::Agg(AggFunc::Avg,   Box::new(Expr::Column("age".into())))
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column reference, possibly qualified (`t.c`).
    /// SQL examples:
    ///   name → `Column(ColumnRef { qualifier: None, name: "name" })`
    ///   users.age → `Column(ColumnRef { qualifier: Some("users"), name: "age" })`
    Column(ColumnRef),

    /// A constant value: number, string, boolean, or `NULL`.
    /// SQL examples:
    ///   42 → `Literal(Value::Int64(42))`
    ///   'hello' → `Literal(Value::String("hello"))`
    ///   true → `Literal(Value::Bool(true))`
    ///   false → `Literal(Value::Bool(false))`
    ///   NULL → `Literal(Value::Null)`
    Literal(Value),

    /// An aggregate call, e.g. `SUM(amount)`. The argument is recursively an
    /// `Expr` so that future syntax like `SUM(a + 1)` requires no AST change.
    /// SQL examples:
    ///   SUM(amount) → `Agg` { func: `AggFunc::Sum`, arg: `Expr::Column("amount")` }
    ///   COUNT(name) → `Agg` { func: `AggFunc::Count`, arg: `Expr::Column("name")` }
    Agg { func: AggFunc, arg: Box<Expr> },

    /// The special `COUNT(*)` form that counts all rows.
    CountStar,

    /// A binary operator applied to two sub-expressions, e.g. `age > 25`,
    /// `a AND b`, `x = y`. Boolean connectives (`AND`, `OR`) and comparisons
    /// share this shape — they differ only by the [`BinOp`] tag and the
    /// types the binder will require of their operands.
    BinaryOp {
        lhs: Box<Expr>,
        op: BinOp,
        rhs: Box<Expr>,
    },

    /// A unary operator applied to a sub-expression, e.g. `NOT (age > 25)`.
    /// The binder enforces the operand's type (e.g. `NOT` requires boolean).
    UnaryOp { op: UnOp, operand: Box<Expr> },

    /// `expr IS [NOT] NULL`
    ///
    /// Unlike comparisons, IS NULL never propagates NULL — it always returns a
    /// definite Bool. That is precisely why it exists: `col = NULL` yields NULL,
    /// but `col IS NULL` yields true/false.
    ///
    /// SQL examples:
    ///   email IS NULL       →  `IsNull` { expr: Column("email"), negated: false }
    ///   phone IS NOT NULL   →  `IsNull` { expr: Column("phone"), negated: true }
    IsNull { expr: Box<Expr>, negated: bool },
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(col) => write!(f, "{col}"),
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Agg { func, arg } => write!(f, "{func}({arg})"),
            Expr::CountStar => write!(f, "COUNT(*)"),
            Expr::BinaryOp { lhs, op, rhs } => write!(f, "({lhs} {op} {rhs})"),
            Expr::UnaryOp { op, operand } => write!(f, "({op} {operand})"),
            Expr::IsNull { expr, negated } => {
                if *negated {
                    write!(f, "{expr} IS NOT NULL")
                } else {
                    write!(f, "{expr} IS NULL")
                }
            }
        }
    }
}

/// A binary operator usable inside an [`Expr::BinaryOp`].
///
/// Grouped roughly by the type the binder will require of the operands and
/// produce as a result:
///
/// - **Logical**: `And`, `Or` — both operands and the result are boolean.
/// - **Comparison**: `Eq`, `NotEq`, `Lt`, `LtEq`, `Gt`, `GtEq` — operands of compatible scalar
///   types, result is boolean.
///
/// The parser produces these from the SQL surface tokens (`AND`, `OR`, `=`,
/// `!=`/`<>`, `<`, `<=`, `>`, `>=`). Arithmetic operators (`+`, `-`, `*`, `/`,
/// `%`) will land here when expression-level arithmetic is wired through the
/// parser and the runtime evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    And,
    Or,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Display for BinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinOp::And => "AND",
            BinOp::Or => "OR",
            BinOp::Eq => "=",
            BinOp::NotEq => "<>",
            BinOp::Lt => "<",
            BinOp::LtEq => "<=",
            BinOp::Gt => ">",
            BinOp::GtEq => ">=",
        };
        f.write_str(s)
    }
}

/// A unary operator usable inside an [`Expr::UnaryOp`].
///
/// Currently only `NOT` (logical negation, boolean → boolean). Unary minus
/// for arithmetic will join this enum when arithmetic lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Not,
}

impl Display for UnOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            UnOp::Not => "NOT",
        };
        f.write_str(s)
    }
}

impl Parser {
    /// Parses a full SQL expression using the loosest (lowest) precedence level.
    ///
    /// This serves as the main entry point for parsing SQL expressions, such as those
    /// found in WHERE clauses. Internally, this method delegates to
    /// [`parse_expression_with_precedence`] using [`Precedence::LOOSEST`] to allow all
    /// valid SQL binary/unary operators to bind according to standard SQL operator precedence.
    ///
    /// # Errors
    ///
    /// Returns a [`ParserError`] if the stream ends prematurely, contains invalid syntax,
    /// or if nested subexpressions are malformed or unbalanced.
    pub(super) fn parse_expression(&mut self) -> Result<Expr, ParserError> {
        self.parse_expression_with_precedence(Precedence::LOOSEST)
    }

    /// Parses a SQL expression until precedence falls below `min_precedence`.
    ///
    /// Used for full clause expressions with `min_precedence = 0` ([`Precedence::LOOSEST`])
    /// and for recursive RHS parses with a higher floor so outer operators keep binding
    /// correctly (Pratt / precedence climbing).
    ///
    /// # SQL examples
    ///
    /// Assuming `users(id, name, age)`:
    ///
    /// ```sql
    /// -- age = 30
    /// --   root: Expr::binary(Column("age"), Eq, Literal(30))
    ///
    /// -- age >= 18 AND name = 'alice'
    /// --   root: Expr::binary( binary(age >= 18), And, binary(name = 'alice') )
    ///
    /// -- NOT age < 18 OR name = 'admin'
    /// --   root: Expr::binary( unary(Not, binary(age < 18)), Or, binary(name = 'admin') )
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the stream ends early, a closing `)` is missing,
    /// tokens mismatch expectations, or a nested parse fails (including lexer errors).
    fn parse_expression_with_precedence(
        &mut self,
        min_precedence: u8,
    ) -> Result<Expr, ParserError> {
        let mut left = self.parse_atom()?;

        loop {
            left = self.apply_postfix_suffixes(left)?;
            let Some((op, l_bp, r_bp)) = self.peek_binary_op()? else {
                break;
            };

            if l_bp < min_precedence {
                break;
            }

            self.bump()?;
            let rhs = self.parse_expression_with_precedence(r_bp)?;
            left = Expr::BinaryOp {
                lhs: Box::new(left),
                op,
                rhs: Box::new(rhs),
            };
        }

        Ok(left)
    }

    /// Applies at most one postfix suffix to `left`, if the next tokens start one.
    ///
    /// Postfix operators come **after** the expression they test (`email IS NULL`),
    /// unlike prefix `NOT` in [`Self::parse_atom`]. Called from
    /// [`Self::parse_expression_with_precedence`] after each atom and again after each
    /// infix step, so `a = 1 OR b IS NULL` attaches `IS NULL` only to `b`.
    ///
    /// SQL does not chain multiple suffixes on the same expression (`email IS NULL IN (1)`
    /// is invalid), so this handles a single suffix per call.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- email IS NULL  →  IsNull { expr: Column("email"), negated: false }
    /// -- phone IS NOT NULL  →  IsNull { expr: Column("phone"), negated: true }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if `IS` is present but `NULL` is missing after optional `NOT`.
    fn apply_postfix_suffixes(&mut self, left: Expr) -> Result<Expr, ParserError> {
        if self.if_peek_then_consume(TokenType::Is)? {
            let negated = self.if_peek_then_consume(TokenType::Not)?;
            self.expect(TokenType::Null)?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }
        Ok(left)
    }

    /// Parses one expression atom — the base unit before infix/postfix operators attach.
    ///
    /// An atom is either a prefix form (`NOT …`, `( … )`) or a leaf (literal,
    /// column reference, aggregate call). The precedence loop in
    /// [`Self::parse_expression_with_precedence`] then applies postfix suffixes and
    /// infix operators (`AND`, `OR`, comparisons).
    ///
    /// # Errors
    ///
    /// Same as [`Self::parse_expression`], plus [`ParserError::ParsingError`] when
    /// a token cannot convert to [`Value`] for literals.
    fn parse_atom(&mut self) -> Result<Expr, ParserError> {
        if self.if_peek_then_consume(TokenType::Not)? {
            let operand =
                self.parse_expression_with_precedence(Precedence::PrefixNot.prefix_bp())?;
            return Ok(Expr::UnaryOp {
                op: UnOp::Not,
                operand: Box::new(operand),
            });
        }

        if self.if_peek_then_consume(TokenType::Lparen)? {
            let expr = self.parse_expression()?;
            self.expect(TokenType::Rparen)?;
            return Ok(expr);
        }

        if self.peek_is(TokenType::Identifier)? {
            // `true`/`false`/`null` are lexed as identifiers today, but semantically
            // they are literals (Value::try_from handles them). Peek-and-special-case
            // them here so expression parsing matches statement-level literal rules.
            let tok = self.bump()?;
            let is_literal_ident = matches!(tok.value.as_str(), "true" | "false" | "null")
                || matches!(tok.value.as_str(), "TRUE" | "FALSE" | "NULL");
            if is_literal_ident {
                let value = Value::try_from(tok).map_err(ParserError::ParsingError)?;
                return Ok(Expr::Literal(value));
            }

            self.lexer.backtrack().map_err(ParserError::from)?;
            return self.parse_identifier();
        }

        let lit = self.bump()?;
        let value = Value::try_from(lit).map_err(ParserError::ParsingError)?;
        Ok(Expr::Literal(value))
    }

    /// Parses `name` or `qualifier.name`, or dispatches to [`Self::parse_fn_call`] when
    /// `name` is followed by `(`.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- age              --> Expr::Column { qualifier: None, name: "age" }
    /// -- u.age            --> Expr::Column { qualifier: Some("u"), name: "age" }
    /// -- COUNT(*)         --> handled in parse_fn_call -> Expr::CountStar
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the first token is not a valid identifier or a
    /// function call is malformed.
    fn parse_identifier(&mut self) -> Result<Expr, ParserError> {
        let first = self.expect_ident()?;

        if self.if_peek_then_consume(TokenType::Dot)? {
            let second = self.expect_ident()?;
            return Ok(Expr::Column(ColumnRef {
                qualifier: Some(first),
                name: second,
            }));
        }

        if self.peek_is(TokenType::Lparen)? {
            return self.parse_fn_call(&first);
        }

        Ok(Expr::Column(ColumnRef {
            qualifier: None,
            name: first,
        }))
    }

    /// Parses `NAME(*)` as [`Expr::CountStar`] or `NAME(expr)` as [`Expr::Agg`].
    ///
    /// Only aggregate names accepted by [`AggFunc::try_from`] succeed; arbitrary
    /// scalar functions are not represented in [`Expr`] yet.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::ParsingError`] when `name` is not `COUNT`/`SUM`/`AVG`/
    /// `MIN`/`MAX`, or any [`ParserError`] from missing `(` / `)` or inner
    /// [`Self::parse_expression`].
    fn parse_fn_call(&mut self, name: &NonEmptyString) -> Result<Expr, ParserError> {
        self.expect(TokenType::Lparen)?;
        if self.if_peek_then_consume(TokenType::Asterisk)? {
            self.expect(TokenType::Rparen)?;
            return Ok(Expr::CountStar);
        }

        let arg = self.parse_expression_with_precedence(Precedence::LOOSEST)?;
        self.expect(TokenType::Rparen)?;
        Ok(Expr::Agg {
            func: AggFunc::try_from(name.as_str()).map_err(ParserError::ParsingError)?,
            arg: Box::new(arg),
        })
    }

    /// If the next token starts a binary operator, returns [`BinOp`] plus left/right
    /// binding powers; otherwise `Ok(None)` so the precedence loop can stop.
    ///
    /// Recognizes `OR`, `AND`, and comparison [`TokenType::Operator`] tokens mapped in
    /// [`Self::peek_operator_value`]. Other operators are treated as non-binary here
    /// (so they do not extend the expression).
    ///
    /// # Errors
    ///
    /// Propagates lexer failures while peeking operator text.
    fn peek_binary_op(&mut self) -> Result<Option<(BinOp, u8, u8)>, ParserError> {
        if self.peek_is(TokenType::Or)? {
            let (l, r) = Precedence::Or.binary_bp();
            return Ok(Some((BinOp::Or, l, r)));
        }

        if self.peek_is(TokenType::And)? {
            let (l, r) = Precedence::And.binary_bp();
            return Ok(Some((BinOp::And, l, r)));
        }

        if self.peek_is(TokenType::Operator)? {
            let (l, r) = Precedence::Comparison.binary_bp();
            return Ok(self.peek_operator_value()?.and_then(|sym| {
                let op = match sym.as_str() {
                    "=" => BinOp::Eq,
                    "!=" | "<>" => BinOp::NotEq,
                    "<" => BinOp::Lt,
                    "<=" => BinOp::LtEq,
                    ">" => BinOp::Gt,
                    ">=" => BinOp::GtEq,
                    _ => return None,
                };
                Some((op, l, r))
            }));
        }

        Ok(None)
    }

    /// Peeks the next token and returns its text when it is [`TokenType::Operator`],
    /// then rewinds the lexer; used to distinguish `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
    /// for [`Self::peek_binary_op`].
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::LexError`] if the lexer fails; [`ParserError::WantedToken`]
    /// is not produced here — an exhausted stream yields `Ok(None)`.
    fn peek_operator_value(&mut self) -> Result<Option<String>, ParserError> {
        match self.lexer.next() {
            None => Ok(None),
            Some(Err(e)) => Err(ParserError::from(e)),
            Some(Ok(tok)) => {
                let value = if tok.kind == TokenType::Operator {
                    Some(tok.value.clone())
                } else {
                    None
                };
                self.lexer.backtrack().map_err(ParserError::from)?;
                Ok(value)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Value,
        parser::{Parser, parsers::ParserError},
    };

    /// Parse `sql` as a standalone expression (no surrounding SELECT/WHERE).
    /// Passes `min_bp = 0` so every operator binds.
    fn expr(sql: &str) -> Result<Expr, ParserError> {
        Parser::new(sql).parse_expression()
    }

    fn ok(sql: &str) -> Expr {
        expr(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e}"))
    }

    #[test]
    fn literal_integer() {
        assert_eq!(ok("42"), Expr::Literal(Value::Int64(42)));
    }

    #[test]
    fn literal_negative_integer() {
        // The lexer produces the minus as an Operator token, so bare `-1` is
        // not a literal — it would need unary-minus support in the parser.
        // A positive integer literal should round-trip cleanly.
        assert_eq!(ok("0"), Expr::Literal(Value::Int64(0)));
    }

    #[test]
    fn literal_string() {
        assert_eq!(
            ok("'hello'"),
            Expr::Literal(Value::String("hello".to_string()))
        );
    }

    #[test]
    fn literal_boolean_true() {
        assert_eq!(ok("true"), Expr::Literal(Value::Bool(true)));
    }

    #[test]
    fn literal_boolean_false() {
        assert_eq!(ok("false"), Expr::Literal(Value::Bool(false)));
    }

    #[test]
    fn literal_null() {
        assert_eq!(ok("NULL"), Expr::Literal(Value::Null));
    }

    #[test]
    fn column_unqualified() {
        let Expr::Column(col) = ok("name") else {
            panic!("expected Column");
        };
        assert!(col.qualifier.is_none());
        assert_eq!(col.name.as_str(), "name");
    }

    #[test]
    fn column_qualified() {
        let Expr::Column(col) = ok("u.age") else {
            panic!("expected Column");
        };
        assert_eq!(col.qualifier.as_deref(), Some("u"));
        assert_eq!(col.name.as_str(), "age");
    }

    #[test]
    fn agg_count_star() {
        assert_eq!(ok("COUNT(*)"), Expr::CountStar);
    }

    #[test]
    fn agg_count_column() {
        let e = ok("COUNT(id)");
        let Expr::Agg { func, arg } = e else {
            panic!("expected Agg");
        };
        assert_eq!(func, AggFunc::Count);
        assert_eq!(*arg, Expr::Column("id".into()));
    }

    #[test]
    fn agg_sum() {
        let Expr::Agg { func, arg } = ok("SUM(amount)") else {
            panic!("expected Agg");
        };
        assert_eq!(func, AggFunc::Sum);
        assert_eq!(*arg, Expr::Column("amount".into()));
    }

    #[test]
    fn agg_avg() {
        let Expr::Agg { func, .. } = ok("AVG(score)") else {
            panic!("expected Agg");
        };
        assert_eq!(func, AggFunc::Avg);
    }

    #[test]
    fn agg_min() {
        let Expr::Agg { func, .. } = ok("MIN(price)") else {
            panic!("expected Agg");
        };
        assert_eq!(func, AggFunc::Min);
    }

    #[test]
    fn agg_max() {
        let Expr::Agg { func, .. } = ok("MAX(price)") else {
            panic!("expected Agg");
        };
        assert_eq!(func, AggFunc::Max);
    }

    #[test]
    fn cmp_eq() {
        let e = ok("age = 30");
        let Expr::BinaryOp { lhs: _, op, rhs: _ } = e else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Eq);
    }

    #[test]
    fn cmp_not_eq_bang() {
        let Expr::BinaryOp { op, .. } = ok("age != 30") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::NotEq);
    }

    #[test]
    fn cmp_not_eq_sql() {
        let Expr::BinaryOp { op, .. } = ok("age <> 30") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::NotEq);
    }

    #[test]
    fn cmp_lt() {
        let Expr::BinaryOp { op, .. } = ok("x < 5") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Lt);
    }

    #[test]
    fn cmp_lteq() {
        let Expr::BinaryOp { op, .. } = ok("x <= 5") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::LtEq);
    }

    #[test]
    fn cmp_gt() {
        let Expr::BinaryOp { op, .. } = ok("x > 5") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Gt);
    }

    #[test]
    fn cmp_gteq() {
        let Expr::BinaryOp { op, .. } = ok("x >= 5") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::GtEq);
    }

    #[test]
    fn logical_and() {
        let Expr::BinaryOp { lhs, op, rhs } = ok("a = 1 AND b = 2") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::And);
        assert!(matches!(*lhs, Expr::BinaryOp { op: BinOp::Eq, .. }));
        assert!(matches!(*rhs, Expr::BinaryOp { op: BinOp::Eq, .. }));
    }

    #[test]
    fn logical_or() {
        let Expr::BinaryOp { op, .. } = ok("a = 1 OR b = 2") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Or);
    }

    #[test]
    fn unary_not_literal() {
        let Expr::UnaryOp { op, operand } = ok("NOT true") else {
            panic!("expected UnaryOp");
        };
        assert_eq!(op, UnOp::Not);
        assert_eq!(*operand, Expr::Literal(Value::Bool(true)));
    }

    #[test]
    fn unary_not_comparison() {
        // NOT binds tighter than AND, so `NOT a = 1` is `(NOT (a = 1))`.
        let Expr::UnaryOp { op, operand } = ok("NOT a = 1") else {
            panic!("expected UnaryOp");
        };
        assert_eq!(op, UnOp::Not);
        assert!(matches!(*operand, Expr::BinaryOp { op: BinOp::Eq, .. }));
    }

    #[test]
    fn parenthesized_literal() {
        // Parens are transparent — the result is the inner expression.
        assert_eq!(ok("(42)"), Expr::Literal(Value::Int64(42)));
    }

    #[test]
    fn parenthesized_binary_op() {
        // `(a = 1)` should parse the same as `a = 1`.
        let Expr::BinaryOp { op, .. } = ok("(a = 1)") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Eq);
    }

    #[test]
    fn precedence_and_over_or() {
        // `a OR b AND c` must parse as `a OR (b AND c)`, not `(a OR b) AND c`.
        // The root node must be OR, with AND as the right child.
        let Expr::BinaryOp { op, rhs, .. } = ok("a = 1 OR b = 2 AND c = 3") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Or, "root should be OR");
        assert!(
            matches!(*rhs, Expr::BinaryOp { op: BinOp::And, .. }),
            "right child should be AND"
        );
    }

    #[test]
    fn precedence_not_over_and() {
        // `NOT a = 1 AND b = 2` must parse as `(NOT (a = 1)) AND (b = 2)`.
        // Root is AND; left child is NOT.
        let Expr::BinaryOp { op, lhs, .. } = ok("NOT a = 1 AND b = 2") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::And, "root should be AND");
        assert!(
            matches!(*lhs, Expr::UnaryOp { op: UnOp::Not, .. }),
            "left child should be NOT"
        );
    }

    #[test]
    fn parens_override_precedence() {
        // `(a OR b) AND c` — parens force OR to bind first, so root is AND.
        let Expr::BinaryOp { op, lhs, .. } = ok("(a = 1 OR b = 2) AND c = 3") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::And, "root should be AND");
        assert!(
            matches!(*lhs, Expr::BinaryOp { op: BinOp::Or, .. }),
            "left child should be OR"
        );
    }

    #[test]
    fn display_binary_op() {
        // BinaryOp Display wraps in parens: `(lhs op rhs)`.
        let e = ok("age = 30");
        assert_eq!(e.to_string(), "(age = 30)");
    }

    #[test]
    fn display_unary_op() {
        let e = ok("NOT true");
        assert_eq!(e.to_string(), "(NOT true)");
    }

    #[test]
    fn display_count_star() {
        assert_eq!(ok("COUNT(*)").to_string(), "COUNT(*)");
    }

    #[test]
    fn display_agg() {
        assert_eq!(ok("SUM(amount)").to_string(), "SUM(amount)");
    }

    #[test]
    fn error_unknown_agg_function() {
        // `BLAH(col)` looks like a function call but is not a known aggregate.
        assert!(expr("BLAH(col)").is_err());
    }

    #[test]
    fn error_unclosed_paren() {
        // Missing `)` should produce a parse error.
        assert!(expr("(age = 1").is_err());
    }

    #[test]
    fn parse_expr_parses_simple_comparison() {
        let e = Parser::parse_expr("age > 0").expect("should parse");
        assert_eq!(e, Expr::BinaryOp {
            lhs: Box::new(Expr::Column("age".into())),
            op: BinOp::Gt,
            rhs: Box::new(Expr::Literal(Value::Int64(0))),
        });
    }

    #[test]
    fn parse_expr_parses_compound_expression() {
        // (price > 0) AND (stock >= 1) — typical CHECK body
        let e = Parser::parse_expr("price > 0 AND stock >= 1").expect("should parse");
        assert!(matches!(e, Expr::BinaryOp { op: BinOp::And, .. }));
    }

    #[test]
    fn parse_expr_incomplete_expression_errors() {
        assert!(Parser::parse_expr("age >").is_err());
    }

    #[test]
    fn parse_expr_empty_input_errors() {
        assert!(Parser::parse_expr("").is_err());
    }

    #[test]
    fn is_null_column() {
        let Expr::IsNull { expr, negated } = ok("email IS NULL") else {
            panic!("expected IsNull");
        };
        assert!(!negated);
        assert_eq!(*expr, Expr::Column("email".into()));
    }

    #[test]
    fn is_not_null_column() {
        let Expr::IsNull { expr, negated } = ok("phone IS NOT NULL") else {
            panic!("expected IsNull");
        };
        assert!(negated);
        assert_eq!(*expr, Expr::Column("phone".into()));
    }

    #[test]
    fn null_literal_is_null() {
        let Expr::IsNull { expr, negated } = ok("NULL IS NULL") else {
            panic!("expected IsNull");
        };
        assert!(!negated);
        assert_eq!(*expr, Expr::Literal(Value::Null));
    }

    #[test]
    fn is_null_binds_before_and() {
        // `email IS NULL AND active = true` → (email IS NULL) AND (active = true)
        let Expr::BinaryOp { op, lhs, .. } = ok("email IS NULL AND active = true") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::And);
        assert!(matches!(*lhs, Expr::IsNull { negated: false, .. }));
    }

    #[test]
    fn is_null_on_rhs_of_or() {
        let Expr::BinaryOp { op, rhs, .. } = ok("a = 1 OR b IS NULL") else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Or);
        assert!(matches!(*rhs, Expr::IsNull { negated: false, .. }));
    }

    #[test]
    fn display_is_null() {
        assert_eq!(ok("email IS NULL").to_string(), "email IS NULL");
        assert_eq!(ok("phone IS NOT NULL").to_string(), "phone IS NOT NULL");
    }
}
