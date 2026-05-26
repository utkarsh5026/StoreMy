//! Expression resolution — lowers a parsed [`Expr`] into a [`ResolvedExpr`]
//! where every column reference has been replaced by a [`ColumnId`].
//!
//! # Why this exists
//!
//! The parser produces [`Expr`] trees whose column references are string names
//! (`ColumnRef { qualifier: Option<String>, name: String }`). Every time
//! `eval_expr` evaluates such a node it must walk the schema to find the
//! matching field — a string comparison per column reference per row.
//!
//! [`ResolvedExpr::resolve`] does that lookup *once*, at bind time, and replaces every
//! `ColumnRef` with the numeric [`ColumnId`] it resolves to. The resulting
//! [`ResolvedExpr`] can then be evaluated by [`ResolvedExpr::eval`] with a plain
//! index lookup — no string comparison, no schema argument, no allocation.
//!
//! # Dependency design
//!
//! This module defines the [`ColumnLookup`] trait rather than importing the
//! engine's `ColumnResolver`. That keeps `execution` free of any dependency on
//! `engine`. The engine implements `ColumnLookup` for `Scope` and
//! `SingleTableScope` in `engine/resolution.rs`.
//!
//! # What is NOT resolved here
//!
//! Aggregate expressions ([`ResolvedExpr::Agg`], [`ResolvedExpr::CountStar`])
//! are carried through structurally — their column arguments are resolved, but
//! they cannot be evaluated row-by-row. The planner is responsible for routing
//! them to the `Aggregate` operator before any row-level evaluation occurs.

use std::cmp::Ordering;

use super::ExecutionError;
use crate::{
    Value,
    parser::statements::{AggFunc, BinOp, CaseBranch, Expr, UnOp},
    primitives::ColumnId,
    tuple::{Tuple, TupleSchema},
    types::{ArithmeticError, Type},
};

/// Minimal interface for mapping a column reference to a [`ColumnId`].
///
/// Defined here in `execution` so that [`ResolvedExpr::resolve`] has no dependency on
/// the engine layer. The engine's `Scope` (multi-table SELECT) and
/// `SingleTableScope` (single-table DML) both implement this trait.
///
/// Returns `None` when the column is unknown or ambiguous — [`ResolvedExpr::resolve`]
/// converts that into an [`ExecutionError`].
pub trait ColumnLookup {
    fn lookup(&self, qualifier: Option<&str>, name: &str) -> Option<ColumnId>;
}

/// An expression with all column references pre-resolved to [`ColumnId`]s.
///
/// Structurally identical to [`Expr`] except:
/// - `Column(ColumnRef)` → `Column(ColumnId)` — string name replaced by index.
/// - [`CaseBranch`] → [`ResolvedCaseBranch`] — branches over `ResolvedExpr`.
///
/// Produced by [`ResolvedExpr::resolve`]; evaluated by [`ResolvedExpr::eval`], which
/// needs only `&Tuple` — no schema parameter, no string lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedExpr {
    /// A column value fetched by its pre-resolved physical index.
    Column(ColumnId),

    /// A constant value baked into the expression tree.
    Literal(Value),

    /// A binary operator applied to two sub-expressions.
    BinaryOp {
        lhs: Box<ResolvedExpr>,
        op: BinOp,
        rhs: Box<ResolvedExpr>,
    },

    /// A unary operator (`NOT`) applied to one sub-expression.
    UnaryOp {
        op: UnOp,
        operand: Box<ResolvedExpr>,
    },

    /// `expr IS [NOT] NULL`
    IsNull {
        expr: Box<ResolvedExpr>,
        negated: bool,
    },

    /// `expr [NOT] IN (val, …)`
    In {
        expr: Box<ResolvedExpr>,
        list: Vec<ResolvedExpr>,
        negated: bool,
    },

    /// `expr [NOT] BETWEEN low AND high`
    Between {
        expr: Box<ResolvedExpr>,
        low: Box<ResolvedExpr>,
        high: Box<ResolvedExpr>,
        negated: bool,
    },

    /// `expr [NOT] LIKE pattern`
    Like {
        expr: Box<ResolvedExpr>,
        pattern: Box<ResolvedExpr>,
        negated: bool,
    },

    /// `CASE [operand] WHEN … THEN … [ELSE …] END`
    Case {
        operand: Option<Box<ResolvedExpr>>,
        branches: Vec<ResolvedCaseBranch>,
        else_result: Option<Box<ResolvedExpr>>,
    },

    /// A non-`COUNT(*)` aggregate call. The argument column is resolved.
    /// Cannot be evaluated row-by-row — must be handled by the `Aggregate` operator.
    Agg {
        func: AggFunc,
        arg: Box<ResolvedExpr>,
    },

    /// `COUNT(*)` — counts every row. Cannot be evaluated row-by-row.
    CountStar,
}

/// One `WHEN … THEN …` branch inside a [`ResolvedExpr::Case`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCaseBranch {
    pub when: ResolvedExpr,
    pub then: ResolvedExpr,
}

impl ResolvedExpr {
    /// Evaluates this expression against a single tuple row.
    ///
    /// Every column reference is a plain index into `tuple` — no schema scan,
    /// no string comparison.
    ///
    /// # Errors
    ///
    /// - [`ExecutionError::TypeError`] if a column index is out of bounds.
    /// - [`ExecutionError::TypeError`] if `AND`/`OR`/`NOT` receives a non-Bool operand.
    /// - [`ExecutionError::TypeError`] if `LIKE` receives a non-String operand.
    /// - [`ExecutionError::TypeError`] if an aggregate node is encountered — those require many
    ///   rows and must be handled by the `Aggregate` operator first.
    #[allow(clippy::too_many_lines)]
    pub fn eval(&self, tuple: &Tuple) -> Result<Value, ExecutionError> {
        match self {
            Self::Column(id) => {
                let idx = usize::from(*id);
                tuple.get(idx).cloned().ok_or_else(|| {
                    ExecutionError::TypeError(format!("column index {idx} out of bounds"))
                })
            }

            Self::Literal(v) => Ok(v.clone()),

            Self::BinaryOp { lhs, op, rhs } => {
                let l = lhs.eval(tuple)?;
                let r = rhs.eval(tuple)?;
                eval_binary(*op, &l, &r)
            }

            Self::UnaryOp { op, operand } => {
                let v = operand.eval(tuple)?;
                if v.is_null() {
                    return Ok(Value::Null);
                }
                match op {
                    UnOp::Not => Ok(Value::Bool(!as_bool(&v, "NOT")?)),
                }
            }

            Self::IsNull { expr, negated } => {
                let v = expr.eval(tuple)?;
                Ok(Value::Bool(if *negated {
                    !v.is_null()
                } else {
                    v.is_null()
                }))
            }

            Self::In {
                expr,
                list,
                negated,
            } => {
                let v = expr.eval(tuple)?;
                if v.is_null() {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for e in list {
                    let item = e.eval(tuple)?;
                    if item.is_null() {
                        saw_null = true;
                        continue;
                    }
                    if item == v {
                        return Ok(Value::Bool(!negated));
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }

            Self::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = expr.eval(tuple)?;
                let lo = low.eval(tuple)?;
                let hi = high.eval(tuple)?;
                if v.is_null() || lo.is_null() || hi.is_null() {
                    return Ok(Value::Null);
                }
                match (lo.partial_cmp(&v), v.partial_cmp(&hi)) {
                    (Some(lo_ord), Some(hi_ord)) => {
                        let in_range = lo_ord.is_le() && hi_ord.is_le();
                        Ok(Value::Bool(if *negated { !in_range } else { in_range }))
                    }
                    _ => Ok(Value::Null),
                }
            }

            Self::Like {
                expr,
                pattern,
                negated,
            } => {
                let text = expr.eval(tuple)?;
                let pat = pattern.eval(tuple)?;
                if text.is_null() || pat.is_null() {
                    return Ok(Value::Null);
                }
                let Value::String(text_str) = text else {
                    return Err(ExecutionError::TypeError(
                        "LIKE requires a String left-hand side".to_string(),
                    ));
                };
                let Value::String(pat_str) = pat else {
                    return Err(ExecutionError::TypeError(
                        "LIKE requires a String pattern".to_string(),
                    ));
                };
                let matched = like_matches(&text_str, &pat_str);
                Ok(Value::Bool(if *negated { !matched } else { matched }))
            }

            Self::Case {
                operand,
                branches,
                else_result,
            } => {
                let base = operand.as_deref().map(|e| e.eval(tuple)).transpose()?;

                for ResolvedCaseBranch { when, then } in branches {
                    let condition_holds = match &base {
                        None => when.eval_bool(tuple)?,
                        Some(base_val) => {
                            let v = when.eval(tuple)?;
                            !base_val.is_null() && !v.is_null() && *base_val == v
                        }
                    };
                    if condition_holds {
                        return then.eval(tuple);
                    }
                }
                match else_result {
                    Some(e) => e.eval(tuple),
                    None => Ok(Value::Null),
                }
            }

            Self::Agg { .. } | Self::CountStar => Err(ExecutionError::TypeError(
                "aggregate expressions cannot be evaluated as scalar expressions".to_string(),
            )),
        }
    }

    /// Evaluates this expression and returns `true` iff the result is `Value::Bool(true)`.
    ///
    /// All other results — `false`, `NULL`, any non-boolean — return `false`.
    /// This is the SQL predicate check used by `WHERE`, join conditions, and residual filters.
    #[inline]
    pub fn eval_bool(&self, tuple: &Tuple) -> Result<bool, ExecutionError> {
        Ok(matches!(self.eval(tuple)?, Value::Bool(true)))
    }

    /// Statically infers the output [`Type`] of this expression without evaluating any rows.
    ///
    /// Used to build output schemas for `Project` and `Aggregate` before the first row is pulled.
    ///
    /// - `Column` — inherits the field type from `schema`.
    /// - `Literal` — derives the type from the [`Value`] variant; `NULL` falls back to `String`.
    /// - `BinaryOp` arithmetic (`+`, `-`, `*`, `/`) — follows the left operand (conservative).
    /// - All boolean-valued operators (`=`, `<`, `AND`, `BETWEEN`, `LIKE`, …) → `Bool`.
    /// - Aggregates and anything else fall back to `String`.
    pub fn infer_type(&self, schema: &TupleSchema) -> Type {
        match self {
            Self::Column(id) => schema
                .field_or_err(usize::from(*id))
                .map(|f| f.field_type)
                .unwrap_or(Type::String),
            Self::Literal(v) => v.get_type().unwrap_or(Type::String),
            Self::BinaryOp { lhs, op, .. } => match op {
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => lhs.infer_type(schema),
                _ => Type::Bool,
            },
            Self::UnaryOp { .. }
            | Self::IsNull { .. }
            | Self::In { .. }
            | Self::Between { .. }
            | Self::Like { .. } => Type::Bool,
            _ => Type::String,
        }
    }

    /// Resolves a parsed [`Expr`] into a `ResolvedExpr` by replacing every
    /// `ColumnRef` with the [`ColumnId`] that `resolver` returns.
    ///
    /// This is the entry point for bind-time column resolution.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::TypeError`] when `resolver` returns `None` for
    /// any column reference (unknown or ambiguous column).
    pub fn resolve(expr: Expr, resolver: &impl ColumnLookup) -> Result<Self, ExecutionError> {
        match expr {
            Expr::Column(col_ref) => {
                let id = resolver
                    .lookup(col_ref.qualifier.as_deref(), col_ref.name.as_str())
                    .ok_or_else(|| {
                        ExecutionError::TypeError(format!("unknown column '{}'", col_ref.name))
                    })?;
                Ok(Self::Column(id))
            }

            Expr::Literal(v) => Ok(Self::Literal(v)),

            Expr::BinaryOp { lhs, op, rhs } => Ok(Self::BinaryOp {
                lhs: Box::new(Self::resolve(*lhs, resolver)?),
                op,
                rhs: Box::new(Self::resolve(*rhs, resolver)?),
            }),

            Expr::UnaryOp { op, operand } => Ok(Self::UnaryOp {
                op,
                operand: Box::new(Self::resolve(*operand, resolver)?),
            }),

            Expr::IsNull { expr, negated } => Ok(Self::IsNull {
                expr: Box::new(Self::resolve(*expr, resolver)?),
                negated,
            }),

            Expr::In {
                expr,
                list,
                negated,
            } => Ok(Self::In {
                expr: Box::new(Self::resolve(*expr, resolver)?),
                list: list
                    .into_iter()
                    .map(|e| Self::resolve(e, resolver))
                    .collect::<Result<Vec<_>, _>>()?,
                negated,
            }),

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Ok(Self::Between {
                expr: Box::new(Self::resolve(*expr, resolver)?),
                low: Box::new(Self::resolve(*low, resolver)?),
                high: Box::new(Self::resolve(*high, resolver)?),
                negated,
            }),

            Expr::Like {
                expr,
                pattern,
                negated,
            } => Ok(Self::Like {
                expr: Box::new(Self::resolve(*expr, resolver)?),
                pattern: Box::new(Self::resolve(*pattern, resolver)?),
                negated,
            }),

            Expr::Case {
                operand,
                branches,
                else_result,
            } => Ok(Self::Case {
                operand: operand
                    .map(|e| Self::resolve(*e, resolver).map(Box::new))
                    .transpose()?,
                branches: branches
                    .into_iter()
                    .map(|CaseBranch { when, then }| {
                        Ok(ResolvedCaseBranch {
                            when: Self::resolve(when, resolver)?,
                            then: Self::resolve(then, resolver)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ExecutionError>>()?,
                else_result: else_result
                    .map(|e| Self::resolve(*e, resolver).map(Box::new))
                    .transpose()?,
            }),

            Expr::Agg { func, arg } => Ok(Self::Agg {
                func,
                arg: Box::new(Self::resolve(*arg, resolver)?),
            }),

            Expr::CountStar => Ok(Self::CountStar),
        }
    }

    /// Rewrites every `Column(id)` leaf in the expression tree by applying `f`,
    /// leaving all other nodes structurally identical.
    ///
    /// `F: Copy` lets the same closure be passed into every recursive call
    /// without cloning — the compiler copies the function pointer / small capture.
    #[must_use]
    pub fn map_columns<F>(self, f: F) -> Self
    where
        F: Fn(ColumnId) -> ColumnId + Copy,
    {
        match self {
            Self::Column(id) => Self::Column(f(id)),
            Self::Literal(_) | Self::CountStar => self,
            Self::BinaryOp { lhs, op, rhs } => Self::BinaryOp {
                lhs: Box::new(lhs.map_columns(f)),
                op,
                rhs: Box::new(rhs.map_columns(f)),
            },
            Self::UnaryOp { op, operand } => Self::UnaryOp {
                op,
                operand: Box::new(operand.map_columns(f)),
            },
            Self::IsNull { expr, negated } => Self::IsNull {
                expr: Box::new(expr.map_columns(f)),
                negated,
            },
            Self::In {
                expr,
                list,
                negated,
            } => Self::In {
                expr: Box::new(expr.map_columns(f)),
                list: list.into_iter().map(|e| e.map_columns(f)).collect(),
                negated,
            },
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => Self::Between {
                expr: Box::new(expr.map_columns(f)),
                low: Box::new(low.map_columns(f)),
                high: Box::new(high.map_columns(f)),
                negated,
            },
            Self::Like {
                expr,
                pattern,
                negated,
            } => Self::Like {
                expr: Box::new(expr.map_columns(f)),
                pattern: Box::new(pattern.map_columns(f)),
                negated,
            },
            Self::Case {
                operand,
                branches,
                else_result,
            } => Self::Case {
                operand: operand.map(|e| Box::new(e.map_columns(f))),
                branches: branches
                    .into_iter()
                    .map(|b| ResolvedCaseBranch {
                        when: b.when.map_columns(f),
                        then: b.then.map_columns(f),
                    })
                    .collect(),
                else_result: else_result.map(|e| Box::new(e.map_columns(f))),
            },
            Self::Agg { func, arg } => Self::Agg {
                func,
                arg: Box::new(arg.map_columns(f)),
            },
        }
    }
}

fn eval_binary(op: BinOp, l: &Value, r: &Value) -> Result<Value, ExecutionError> {
    if l.is_null() || r.is_null() {
        return Ok(Value::Null);
    }
    match op {
        BinOp::And => {
            let lb = as_bool(l, "AND")?;
            let rb = as_bool(r, "AND")?;
            Ok(Value::Bool(lb && rb))
        }
        BinOp::Or => {
            let lb = as_bool(l, "OR")?;
            let rb = as_bool(r, "OR")?;
            Ok(Value::Bool(lb || rb))
        }
        BinOp::Eq => Ok(Value::Bool(l == r)),
        BinOp::NotEq => Ok(Value::Bool(l != r)),
        BinOp::Lt => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_lt)),
        BinOp::LtEq => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_le)),
        BinOp::Gt => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_gt)),
        BinOp::GtEq => Ok(cmp_to_bool(l.partial_cmp(r), Ordering::is_ge)),
        BinOp::Add => l
            .checked_add(r)
            .map_err(|_| ExecutionError::TypeError(format!("cannot add {l} and {r}"))),
        BinOp::Sub => {
            let out = l.clone() - r;
            if out.is_null() {
                Err(ExecutionError::TypeError(format!(
                    "cannot subtract {r} from {l}"
                )))
            } else {
                Ok(out)
            }
        }
        BinOp::Mul => {
            let out = l.clone() * r;
            if out.is_null() {
                Err(ExecutionError::TypeError(format!(
                    "cannot multiply {l} and {r}"
                )))
            } else {
                Ok(out)
            }
        }
        BinOp::Div => l.checked_div(r).map_err(|e| match e {
            ArithmeticError::DivisionByZero => {
                ExecutionError::TypeError("division by zero".to_string())
            }
            ArithmeticError::TypeMismatch => {
                ExecutionError::TypeError(format!("cannot divide {l} by {r}"))
            }
        }),
    }
}

fn cmp_to_bool(ord: Option<Ordering>, pred: fn(Ordering) -> bool) -> Value {
    match ord {
        Some(o) => Value::Bool(pred(o)),
        None => Value::Null,
    }
}

fn as_bool(v: &Value, op_name: &str) -> Result<bool, ExecutionError> {
    v.as_bool().ok_or_else(|| {
        ExecutionError::TypeError(format!("{op_name} requires Bool operands, got {v}"))
    })
}

fn like_matches(text: &str, pattern: &str) -> bool {
    let mut p_chars = pattern.chars();
    let Some(p_head) = p_chars.next() else {
        return text.is_empty();
    };
    let p_tail = p_chars.as_str();
    match p_head {
        '%' => {
            if like_matches(text, p_tail) {
                return true;
            }
            let mut t_chars = text.chars();
            while t_chars.next().is_some() {
                if like_matches(t_chars.as_str(), p_tail) {
                    return true;
                }
            }
            false
        }
        '_' => {
            let mut t_chars = text.chars();
            t_chars
                .next()
                .is_some_and(|_| like_matches(t_chars.as_str(), p_tail))
        }
        literal => {
            let mut t_chars = text.chars();
            match t_chars.next() {
                Some(tc) if tc == literal => like_matches(t_chars.as_str(), p_tail),
                _ => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Type, Value,
        parser::statements::{BinOp, ColumnRef, Expr},
        primitives::ColumnId,
        tuple::{Field, TupleSchema},
    };

    struct SchemaLookup(TupleSchema);

    impl ColumnLookup for SchemaLookup {
        fn lookup(&self, _qualifier: Option<&str>, name: &str) -> Option<ColumnId> {
            self.0.field_by_name(name).map(|(id, _)| id)
        }
    }

    fn schema(fields: &[(&str, Type)]) -> SchemaLookup {
        use crate::primitives::NonEmptyString;
        SchemaLookup(TupleSchema::new(
            fields
                .iter()
                .map(|(name, ty)| Field::new_non_empty(NonEmptyString::new(*name).unwrap(), *ty))
                .collect(),
        ))
    }

    fn col(name: &str) -> Expr {
        Expr::Column(ColumnRef::from(name))
    }

    fn col_id(n: u32) -> ColumnId {
        ColumnId::try_from(n as usize).unwrap()
    }

    #[test]
    fn literal_passes_through() {
        let r = schema(&[("x", Type::Int64)]);
        let resolved = ResolvedExpr::resolve(Expr::Literal(Value::Int64(42)), &r).unwrap();
        assert_eq!(resolved, ResolvedExpr::Literal(Value::Int64(42)));
    }

    #[test]
    fn column_resolves_to_id() {
        let r = schema(&[
            ("id", Type::Int64),
            ("name", Type::String),
            ("age", Type::Int64),
        ]);
        let resolved = ResolvedExpr::resolve(col("age"), &r).unwrap();
        assert_eq!(resolved, ResolvedExpr::Column(col_id(2)));
    }

    #[test]
    fn unknown_column_errors() {
        let r = schema(&[("id", Type::Int64)]);
        let err = ResolvedExpr::resolve(col("nope"), &r).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn binary_op_recurses() {
        let r = schema(&[("age", Type::Int64)]);
        let expr = Expr::BinaryOp {
            lhs: Box::new(col("age")),
            op: BinOp::Gt,
            rhs: Box::new(Expr::Literal(Value::Int64(18))),
        };
        let resolved = ResolvedExpr::resolve(expr, &r).unwrap();
        assert_eq!(resolved, ResolvedExpr::BinaryOp {
            lhs: Box::new(ResolvedExpr::Column(col_id(0))),
            op: BinOp::Gt,
            rhs: Box::new(ResolvedExpr::Literal(Value::Int64(18))),
        });
    }

    #[test]
    fn in_list_resolves_all_elements() {
        let r = schema(&[("id", Type::Int64)]);
        let expr = Expr::In {
            expr: Box::new(col("id")),
            list: vec![
                Expr::Literal(Value::Int64(1)),
                Expr::Literal(Value::Int64(2)),
            ],
            negated: false,
        };
        let resolved = ResolvedExpr::resolve(expr, &r).unwrap();
        let ResolvedExpr::In {
            expr,
            list,
            negated,
        } = resolved
        else {
            panic!("expected In");
        };
        assert_eq!(*expr, ResolvedExpr::Column(col_id(0)));
        assert_eq!(list.len(), 2);
        assert!(!negated);
    }

    #[test]
    fn is_null_resolves_inner() {
        let r = schema(&[("email", Type::String)]);
        let expr = Expr::IsNull {
            expr: Box::new(col("email")),
            negated: false,
        };
        let resolved = ResolvedExpr::resolve(expr, &r).unwrap();
        assert_eq!(resolved, ResolvedExpr::IsNull {
            expr: Box::new(ResolvedExpr::Column(col_id(0))),
            negated: false,
        });
    }

    #[test]
    fn between_resolves_all_three() {
        let r = schema(&[("age", Type::Int64)]);
        let expr = Expr::Between {
            expr: Box::new(col("age")),
            low: Box::new(Expr::Literal(Value::Int64(18))),
            high: Box::new(Expr::Literal(Value::Int64(65))),
            negated: false,
        };
        let ResolvedExpr::Between {
            expr, low, high, ..
        } = ResolvedExpr::resolve(expr, &r).unwrap()
        else {
            panic!("expected Between");
        };
        assert_eq!(*expr, ResolvedExpr::Column(col_id(0)));
        assert_eq!(*low, ResolvedExpr::Literal(Value::Int64(18)));
        assert_eq!(*high, ResolvedExpr::Literal(Value::Int64(65)));
    }

    #[test]
    fn case_searched_resolves_branches() {
        let r = schema(&[("x", Type::Int64)]);
        let expr = Expr::Case {
            operand: None,
            branches: vec![CaseBranch {
                when: Expr::BinaryOp {
                    lhs: Box::new(col("x")),
                    op: BinOp::Eq,
                    rhs: Box::new(Expr::Literal(Value::Int64(1))),
                },
                then: Expr::Literal(Value::Int64(10)),
            }],
            else_result: Some(Box::new(Expr::Literal(Value::Int64(0)))),
        };
        let ResolvedExpr::Case {
            operand,
            branches,
            else_result,
        } = ResolvedExpr::resolve(expr, &r).unwrap()
        else {
            panic!("expected Case");
        };
        assert!(operand.is_none());
        assert_eq!(branches.len(), 1);
        assert!(matches!(branches[0].when, ResolvedExpr::BinaryOp {
            op: BinOp::Eq,
            ..
        }));
        assert_eq!(branches[0].then, ResolvedExpr::Literal(Value::Int64(10)));
        assert_eq!(
            else_result.as_deref(),
            Some(&ResolvedExpr::Literal(Value::Int64(0)))
        );
    }

    #[test]
    fn count_star_passes_through() {
        let r = schema(&[]);
        let resolved = ResolvedExpr::resolve(Expr::CountStar, &r).unwrap();
        assert_eq!(resolved, ResolvedExpr::CountStar);
    }

    #[test]
    fn agg_resolves_argument() {
        use crate::parser::statements::AggFunc;
        let r = schema(&[("amount", Type::Int64)]);
        let expr = Expr::Agg {
            func: AggFunc::Sum,
            arg: Box::new(col("amount")),
        };
        let ResolvedExpr::Agg { func, arg } = ResolvedExpr::resolve(expr, &r).unwrap() else {
            panic!("expected Agg");
        };
        assert_eq!(func, AggFunc::Sum);
        assert_eq!(*arg, ResolvedExpr::Column(col_id(0)));
    }

    #[test]
    fn error_in_nested_expr_propagates() {
        let r = schema(&[("age", Type::Int64)]);
        let expr = Expr::BinaryOp {
            lhs: Box::new(col("age")),
            op: BinOp::And,
            rhs: Box::new(col("nope")),
        };
        assert!(ResolvedExpr::resolve(expr, &r).is_err());
    }

    // ── ResolvedExpr::eval ───────────────────────────────────────────────────

    use crate::tuple::Tuple;

    fn tuple(values: Vec<Value>) -> Tuple {
        Tuple::new(values)
    }

    fn resolved_col(id: u32) -> ResolvedExpr {
        ResolvedExpr::Column(col_id(id))
    }

    fn lit(v: Value) -> ResolvedExpr {
        ResolvedExpr::Literal(v)
    }

    fn eval(expr: &ResolvedExpr, t: &Tuple) -> Value {
        expr.eval(t).expect("eval failed")
    }

    #[test]
    fn eval_literal_returns_value() {
        let t = tuple(vec![]);
        assert_eq!(eval(&lit(Value::Int64(42)), &t), Value::Int64(42));
        assert_eq!(eval(&lit(Value::Null), &t), Value::Null);
    }

    #[test]
    fn eval_column_fetches_by_index() {
        let t = tuple(vec![Value::Int64(7), Value::String("alice".into())]);
        assert_eq!(eval(&resolved_col(0), &t), Value::Int64(7));
        assert_eq!(eval(&resolved_col(1), &t), Value::String("alice".into()));
    }

    #[test]
    fn eval_column_out_of_bounds_errors() {
        let t = tuple(vec![Value::Int64(1)]);
        let err = resolved_col(5).eval(&t).unwrap_err();
        assert!(matches!(err, super::super::ExecutionError::TypeError(_)));
    }

    #[test]
    fn eval_binary_eq() {
        let t = tuple(vec![Value::Int64(5)]);
        let expr = ResolvedExpr::BinaryOp {
            lhs: Box::new(resolved_col(0)),
            op: BinOp::Eq,
            rhs: Box::new(lit(Value::Int64(5))),
        };
        assert_eq!(eval(&expr, &t), Value::Bool(true));
    }

    #[test]
    fn eval_null_propagates_through_comparison() {
        let t = tuple(vec![Value::Null]);
        let expr = ResolvedExpr::BinaryOp {
            lhs: Box::new(resolved_col(0)),
            op: BinOp::Eq,
            rhs: Box::new(lit(Value::Int64(1))),
        };
        assert_eq!(eval(&expr, &t), Value::Null);
    }

    #[test]
    fn eval_is_null_on_null() {
        let t = tuple(vec![Value::Null]);
        let expr = ResolvedExpr::IsNull {
            expr: Box::new(resolved_col(0)),
            negated: false,
        };
        assert_eq!(eval(&expr, &t), Value::Bool(true));
    }

    #[test]
    fn eval_is_not_null_on_value() {
        let t = tuple(vec![Value::Int64(1)]);
        let expr = ResolvedExpr::IsNull {
            expr: Box::new(resolved_col(0)),
            negated: true,
        };
        assert_eq!(eval(&expr, &t), Value::Bool(true));
    }

    #[test]
    fn eval_in_match() {
        let t = tuple(vec![Value::Int64(2)]);
        let expr = ResolvedExpr::In {
            expr: Box::new(resolved_col(0)),
            list: vec![
                lit(Value::Int64(1)),
                lit(Value::Int64(2)),
                lit(Value::Int64(3)),
            ],
            negated: false,
        };
        assert_eq!(eval(&expr, &t), Value::Bool(true));
    }

    #[test]
    fn eval_between_in_range() {
        let t = tuple(vec![Value::Int64(25)]);
        let expr = ResolvedExpr::Between {
            expr: Box::new(resolved_col(0)),
            low: Box::new(lit(Value::Int64(18))),
            high: Box::new(lit(Value::Int64(65))),
            negated: false,
        };
        assert_eq!(eval(&expr, &t), Value::Bool(true));
    }

    #[test]
    fn eval_like_matches() {
        let t = tuple(vec![Value::String("alice".into())]);
        let expr = ResolvedExpr::Like {
            expr: Box::new(resolved_col(0)),
            pattern: Box::new(lit(Value::String("ali%".into()))),
            negated: false,
        };
        assert_eq!(eval(&expr, &t), Value::Bool(true));
    }

    #[test]
    fn eval_case_searched_first_match() {
        let t = tuple(vec![Value::Int64(1)]);
        let expr = ResolvedExpr::Case {
            operand: None,
            branches: vec![
                ResolvedCaseBranch {
                    when: ResolvedExpr::BinaryOp {
                        lhs: Box::new(resolved_col(0)),
                        op: BinOp::Eq,
                        rhs: Box::new(lit(Value::Int64(1))),
                    },
                    then: lit(Value::Int64(10)),
                },
                ResolvedCaseBranch {
                    when: ResolvedExpr::BinaryOp {
                        lhs: Box::new(resolved_col(0)),
                        op: BinOp::Eq,
                        rhs: Box::new(lit(Value::Int64(2))),
                    },
                    then: lit(Value::Int64(20)),
                },
            ],
            else_result: None,
        };
        assert_eq!(eval(&expr, &t), Value::Int64(10));
    }

    #[test]
    fn eval_aggregate_errors() {
        let t = tuple(vec![]);
        let err = ResolvedExpr::CountStar.eval(&t).unwrap_err();
        assert!(matches!(err, super::super::ExecutionError::TypeError(_)));
    }

    fn arith(op: BinOp, l: Value, r: Value) -> Result<Value, ExecutionError> {
        let t = tuple(vec![]);
        ResolvedExpr::BinaryOp {
            lhs: Box::new(lit(l)),
            op,
            rhs: Box::new(lit(r)),
        }
        .eval(&t)
    }

    fn arith_ok(op: BinOp, l: Value, r: Value) -> Value {
        arith(op, l, r).expect("expected Ok from arith")
    }

    #[test]
    fn eval_add_int64() {
        assert_eq!(
            arith_ok(BinOp::Add, Value::Int64(3), Value::Int64(4)),
            Value::Int64(7)
        );
    }

    #[test]
    fn eval_sub_int64() {
        assert_eq!(
            arith_ok(BinOp::Sub, Value::Int64(10), Value::Int64(3)),
            Value::Int64(7)
        );
    }

    #[test]
    fn eval_mul_int64() {
        assert_eq!(
            arith_ok(BinOp::Mul, Value::Int64(6), Value::Int64(7)),
            Value::Int64(42)
        );
    }

    #[test]
    fn eval_div_int64() {
        assert_eq!(
            arith_ok(BinOp::Div, Value::Int64(20), Value::Int64(4)),
            Value::Int64(5)
        );
    }

    #[test]
    fn eval_add_float64() {
        assert_eq!(
            arith_ok(BinOp::Add, Value::Float64(1.5), Value::Float64(2.5)),
            Value::Float64(4.0)
        );
    }

    #[test]
    fn eval_sub_float64() {
        assert_eq!(
            arith_ok(BinOp::Sub, Value::Float64(5.0), Value::Float64(1.5)),
            Value::Float64(3.5)
        );
    }

    #[test]
    fn eval_mul_float64() {
        assert_eq!(
            arith_ok(BinOp::Mul, Value::Float64(2.0), Value::Float64(3.5)),
            Value::Float64(7.0)
        );
    }

    #[test]
    fn eval_div_float64() {
        assert_eq!(
            arith_ok(BinOp::Div, Value::Float64(7.0), Value::Float64(2.0)),
            Value::Float64(3.5)
        );
    }

    #[test]
    fn eval_add_uint64() {
        assert_eq!(
            arith_ok(BinOp::Add, Value::Uint64(10), Value::Uint64(5)),
            Value::Uint64(15)
        );
    }

    #[test]
    fn eval_div_by_zero_int64_errors() {
        let err = arith(BinOp::Div, Value::Int64(5), Value::Int64(0)).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn eval_div_by_zero_uint64_errors() {
        let err = arith(BinOp::Div, Value::Uint64(5), Value::Uint64(0)).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn eval_arith_type_mismatch_errors() {
        let err = arith(BinOp::Add, Value::Int64(1), Value::String("x".into())).unwrap_err();
        assert!(matches!(err, ExecutionError::TypeError(_)));
    }

    #[test]
    fn eval_arith_null_left_propagates() {
        assert_eq!(
            arith_ok(BinOp::Add, Value::Null, Value::Int64(1)),
            Value::Null
        );
    }

    #[test]
    fn eval_arith_null_right_propagates() {
        assert_eq!(
            arith_ok(BinOp::Mul, Value::Int64(5), Value::Null),
            Value::Null
        );
    }

    #[test]
    fn eval_arith_column_plus_literal() {
        // col(0) + 10 where col(0) = 32  →  42
        let t = tuple(vec![Value::Int64(32)]);
        let expr = ResolvedExpr::BinaryOp {
            lhs: Box::new(resolved_col(0)),
            op: BinOp::Add,
            rhs: Box::new(lit(Value::Int64(10))),
        };
        assert_eq!(eval(&expr, &t), Value::Int64(42));
    }

    #[test]
    fn eval_arith_nested_mul_add() {
        // (2 + 3) * 4 = 20 — verifies nested BinaryOp eval
        let t = tuple(vec![]);
        let inner = ResolvedExpr::BinaryOp {
            lhs: Box::new(lit(Value::Int64(2))),
            op: BinOp::Add,
            rhs: Box::new(lit(Value::Int64(3))),
        };
        let expr = ResolvedExpr::BinaryOp {
            lhs: Box::new(inner),
            op: BinOp::Mul,
            rhs: Box::new(lit(Value::Int64(4))),
        };
        assert_eq!(eval(&expr, &t), Value::Int64(20));
    }

    #[test]
    fn map_columns_remaps_column_leaf() {
        // col[0] shifted by +3 → col[3]
        let expr = resolved_col(0);
        let mapped = expr.map_columns(|id| col_id(u32::try_from(usize::from(id)).unwrap() + 3));
        assert_eq!(mapped, ResolvedExpr::Column(col_id(3)));
    }

    #[test]
    fn map_columns_leaves_literal_unchanged() {
        let expr = lit(Value::Int64(99));
        let mapped = expr.clone().map_columns(|_| col_id(99));
        assert_eq!(mapped, expr);
    }

    #[test]
    fn map_columns_recurses_binary_op() {
        // col[0] = col[3]  →  swap so old-left→new-right, old-right→new-left
        // left_width=3, right_width=3
        // col[0] (< 3) → right side: 3 + 0 = 3
        // col[3] (≥ 3) → left  side: 3 - 3 = 0
        let expr = ResolvedExpr::BinaryOp {
            lhs: Box::new(resolved_col(0)),
            op: BinOp::Eq,
            rhs: Box::new(resolved_col(3)),
        };
        let left_width = 3usize;
        let right_width = 3usize;
        let mapped = expr.map_columns(|id| {
            let i = usize::from(id);
            if i < left_width {
                col_id(u32::try_from(right_width + i).unwrap())
            } else {
                col_id(u32::try_from(i - left_width).unwrap())
            }
        });
        assert_eq!(mapped, ResolvedExpr::BinaryOp {
            lhs: Box::new(ResolvedExpr::Column(col_id(3))),
            op: BinOp::Eq,
            rhs: Box::new(ResolvedExpr::Column(col_id(0))),
        });
    }
}
