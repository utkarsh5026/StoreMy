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
//! [`resolve_expr`] does that lookup *once*, at bind time, and replaces every
//! `ColumnRef` with the numeric [`ColumnId`] it resolves to. The resulting
//! [`ResolvedExpr`] can then be evaluated by `eval_resolved_expr` (to be added
//! in Phase 3) with a plain index lookup — no string comparison, no `schema`
//! argument, no allocation.
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
    tuple::Tuple,
};

/// Minimal interface for mapping a column reference to a [`ColumnId`].
///
/// Defined here in `execution` so that [`resolve_expr`] has no dependency on
/// the engine layer. The engine's `Scope` (multi-table SELECT) and
/// `SingleTableScope` (single-table DML) both implement this trait.
///
/// Returns `None` when the column is unknown or ambiguous — [`resolve_expr`]
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
/// Produced by [`resolve_expr`]; evaluated by [`eval_resolved_expr`], which
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

/// Walks an [`Expr`] tree and replaces every `Column(ColumnRef)` node with
/// `Column(ColumnId)` using `resolver`.
///
/// All other nodes are reconstructed recursively — the tree shape is preserved.
///
/// # Errors
///
/// Returns [`ExecutionError::TypeError`] when `resolver` returns `None` for a
/// column reference (unknown or ambiguous column).
pub fn resolve_expr(
    resolver: &impl ColumnLookup,
    expr: Expr,
) -> Result<ResolvedExpr, ExecutionError> {
    match expr {
        Expr::Column(col_ref) => {
            let id = resolver
                .lookup(col_ref.qualifier.as_deref(), col_ref.name.as_str())
                .ok_or_else(|| {
                    ExecutionError::TypeError(format!("unknown column '{}'", col_ref.name))
                })?;
            Ok(ResolvedExpr::Column(id))
        }

        Expr::Literal(v) => Ok(ResolvedExpr::Literal(v)),

        Expr::BinaryOp { lhs, op, rhs } => Ok(ResolvedExpr::BinaryOp {
            lhs: Box::new(resolve_expr(resolver, *lhs)?),
            op,
            rhs: Box::new(resolve_expr(resolver, *rhs)?),
        }),

        Expr::UnaryOp { op, operand } => Ok(ResolvedExpr::UnaryOp {
            op,
            operand: Box::new(resolve_expr(resolver, *operand)?),
        }),

        Expr::IsNull { expr, negated } => Ok(ResolvedExpr::IsNull {
            expr: Box::new(resolve_expr(resolver, *expr)?),
            negated,
        }),

        Expr::In {
            expr,
            list,
            negated,
        } => Ok(ResolvedExpr::In {
            expr: Box::new(resolve_expr(resolver, *expr)?),
            list: list
                .into_iter()
                .map(|e| resolve_expr(resolver, e))
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        }),

        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(ResolvedExpr::Between {
            expr: Box::new(resolve_expr(resolver, *expr)?),
            low: Box::new(resolve_expr(resolver, *low)?),
            high: Box::new(resolve_expr(resolver, *high)?),
            negated,
        }),

        Expr::Like {
            expr,
            pattern,
            negated,
        } => Ok(ResolvedExpr::Like {
            expr: Box::new(resolve_expr(resolver, *expr)?),
            pattern: Box::new(resolve_expr(resolver, *pattern)?),
            negated,
        }),

        Expr::Case {
            operand,
            branches,
            else_result,
        } => Ok(ResolvedExpr::Case {
            operand: operand
                .map(|e| resolve_expr(resolver, *e).map(Box::new))
                .transpose()?,
            branches: branches
                .into_iter()
                .map(|CaseBranch { when, then }| {
                    Ok(ResolvedCaseBranch {
                        when: resolve_expr(resolver, when)?,
                        then: resolve_expr(resolver, then)?,
                    })
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?,
            else_result: else_result
                .map(|e| resolve_expr(resolver, *e).map(Box::new))
                .transpose()?,
        }),

        Expr::Agg { func, arg } => Ok(ResolvedExpr::Agg {
            func,
            arg: Box::new(resolve_expr(resolver, *arg)?),
        }),

        Expr::CountStar => Ok(ResolvedExpr::CountStar),
    }
}

/// Evaluates a [`ResolvedExpr`] against a single tuple row.
///
/// Every column reference is a plain index into `tuple` — no schema scan,
/// no string comparison. All other behavior (NULL propagation, three-valued
/// logic, LIKE matching, CASE branching) is identical to `eval_expr`.
///
/// # Errors
///
/// - [`ExecutionError::TypeError`] if a column index is out of bounds.
/// - [`ExecutionError::TypeError`] if `AND`/`OR`/`NOT` receives a non-Bool operand.
/// - [`ExecutionError::TypeError`] if `LIKE` receives a non-String operand.
/// - [`ExecutionError::TypeError`] if an aggregate node is encountered — those require many rows
///   and must be handled by the `Aggregate` operator first.
#[allow(clippy::too_many_lines)]
pub fn eval_resolved_expr(expr: &ResolvedExpr, tuple: &Tuple) -> Result<Value, ExecutionError> {
    match expr {
        ResolvedExpr::Column(id) => {
            let idx = usize::from(*id);
            tuple.get(idx).cloned().ok_or_else(|| {
                ExecutionError::TypeError(format!("column index {idx} out of bounds"))
            })
        }

        ResolvedExpr::Literal(v) => Ok(v.clone()),

        ResolvedExpr::BinaryOp { lhs, op, rhs } => {
            let l = eval_resolved_expr(lhs, tuple)?;
            let r = eval_resolved_expr(rhs, tuple)?;
            eval_binary(*op, &l, &r)
        }

        ResolvedExpr::UnaryOp { op, operand } => {
            let v = eval_resolved_expr(operand, tuple)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            match op {
                UnOp::Not => Ok(Value::Bool(!as_bool(&v, "NOT")?)),
            }
        }

        ResolvedExpr::IsNull { expr, negated } => {
            let v = eval_resolved_expr(expr, tuple)?;
            Ok(Value::Bool(if *negated {
                !v.is_null()
            } else {
                v.is_null()
            }))
        }

        ResolvedExpr::In {
            expr,
            list,
            negated,
        } => {
            let v = eval_resolved_expr(expr, tuple)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            let mut saw_null = false;
            for e in list {
                let item = eval_resolved_expr(e, tuple)?;
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

        ResolvedExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let v = eval_resolved_expr(expr, tuple)?;
            let lo = eval_resolved_expr(low, tuple)?;
            let hi = eval_resolved_expr(high, tuple)?;
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

        ResolvedExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let text = eval_resolved_expr(expr, tuple)?;
            let pat = eval_resolved_expr(pattern, tuple)?;
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

        ResolvedExpr::Case {
            operand,
            branches,
            else_result,
        } => {
            let base = operand
                .as_deref()
                .map(|e| eval_resolved_expr(e, tuple))
                .transpose()?;

            // Here we resolve all the when then branches and evaluate them against the tuple.
            // If the base is None, we evaluate the when against the tuple and return true if it is
            // true.
            for ResolvedCaseBranch { when, then } in branches {
                let condition_holds = match &base {
                    None => eval_resolved_bool(when, tuple)?,
                    Some(base_val) => {
                        let v = eval_resolved_expr(when, tuple)?;
                        !base_val.is_null() && !v.is_null() && *base_val == v
                    }
                };
                if condition_holds {
                    return eval_resolved_expr(then, tuple);
                }
            }
            // If no branch matches, we evaluate the else result against the tuple.
            match else_result {
                Some(e) => eval_resolved_expr(e, tuple),
                None => Ok(Value::Null),
            }
        }

        ResolvedExpr::Agg { .. } | ResolvedExpr::CountStar => Err(ExecutionError::TypeError(
            "aggregate expressions cannot be evaluated as scalar expressions".to_string(),
        )),
    }
}

/// Evaluates `expr` against `tuple` and returns `true` iff the result is `Value::Bool(true)`.
///
/// All other results — `false`, `NULL`, any non-boolean — return `false`.
/// This is the SQL predicate check used by `WHERE`, join conditions, and residual filters.
#[inline]
pub fn eval_resolved_bool(expr: &ResolvedExpr, tuple: &Tuple) -> Result<bool, ExecutionError> {
    Ok(matches!(
        eval_resolved_expr(expr, tuple)?,
        Value::Bool(true)
    ))
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
        let resolved = resolve_expr(&r, Expr::Literal(Value::Int64(42))).unwrap();
        assert_eq!(resolved, ResolvedExpr::Literal(Value::Int64(42)));
    }

    #[test]
    fn column_resolves_to_id() {
        let r = schema(&[
            ("id", Type::Int64),
            ("name", Type::String),
            ("age", Type::Int64),
        ]);
        let resolved = resolve_expr(&r, col("age")).unwrap();
        assert_eq!(resolved, ResolvedExpr::Column(col_id(2)));
    }

    #[test]
    fn unknown_column_errors() {
        let r = schema(&[("id", Type::Int64)]);
        let err = resolve_expr(&r, col("nope")).unwrap_err();
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
        let resolved = resolve_expr(&r, expr).unwrap();
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
        let resolved = resolve_expr(&r, expr).unwrap();
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
        let resolved = resolve_expr(&r, expr).unwrap();
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
        } = resolve_expr(&r, expr).unwrap()
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
        } = resolve_expr(&r, expr).unwrap()
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
        let resolved = resolve_expr(&r, Expr::CountStar).unwrap();
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
        let ResolvedExpr::Agg { func, arg } = resolve_expr(&r, expr).unwrap() else {
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
        assert!(resolve_expr(&r, expr).is_err());
    }

    // ── eval_resolved_expr ───────────────────────────────────────────────────

    use super::eval_resolved_expr;
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
        eval_resolved_expr(expr, t).expect("eval_resolved_expr failed")
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
        let err = eval_resolved_expr(&resolved_col(5), &t).unwrap_err();
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
        let err = eval_resolved_expr(&ResolvedExpr::CountStar, &t).unwrap_err();
        assert!(matches!(err, super::super::ExecutionError::TypeError(_)));
    }
}
