//! Expressions in the bound AST.
//!
//! Today this only covers literals. Growth points:
//!   - `Column(ColumnBinding)` once SELECT / INSERT..SELECT arrive.
//!   - `Call(FuncId, Vec<BoundExpr>)` when functions land.
//!   - `BinaryOp(Op, Box<BoundExpr>, Box<BoundExpr>)` for arithmetic/string ops.

use crate::Value;

/// An expression appearing in a bound statement.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    /// A constant value. Already coerced to the target column's type by the
    /// binder — the executor does not re-coerce.
    Literal(Value),
}

impl BoundExpr {
    /// Evaluate this expression to a concrete [`Value`].
    ///
    /// For literals this is a clone. Once column refs / params / function
    /// calls arrive, this will take an evaluation context argument.
    pub fn eval(&self) -> Value {
        match self {
            BoundExpr::Literal(v) => v.clone(),
        }
    }
}
