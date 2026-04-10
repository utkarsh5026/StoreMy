use std::cmp::Ordering;

use fallible_iterator::FallibleIterator;

use crate::{
    execution::PlanNode,
    primitives::{self, Predicate},
    tuple::{Tuple, TupleSchema},
    types::Value,
};

use super::{ExecutionError, Executor};

#[derive(Debug)]
pub struct Filter {
    child: Box<PlanNode>,
    col_id: primitives::ColumnId,
    op: Predicate,
    operand: Value,
}

impl Filter {
    fn eval(&self, tuple: &Tuple) -> bool {
        let col_index = usize::from(self.col_id);
        let Some(val) = tuple.get(col_index) else {
            return false;
        };
        match self.op {
            Predicate::Equals => val == &self.operand,
            Predicate::NotEqual | Predicate::NotEqualBracket => val != &self.operand,
            Predicate::LessThan => val.partial_cmp(&self.operand).is_some_and(Ordering::is_lt),
            Predicate::LessThanOrEqual => {
                val.partial_cmp(&self.operand).is_some_and(Ordering::is_le)
            }
            Predicate::GreaterThan => val.partial_cmp(&self.operand).is_some_and(Ordering::is_gt),
            Predicate::GreaterThanOrEqual => {
                val.partial_cmp(&self.operand).is_some_and(Ordering::is_ge)
            }
            Predicate::Like => match (val, &self.operand) {
                (Value::String(s), Value::String(pattern)) => Self::like_match(s, pattern),
                _ => false,
            },
        }
    }

    fn like_match(s: &str, pattern: &str) -> bool {
        let s = s.as_bytes();
        let p = pattern.as_bytes();
        let mut dp = vec![false; p.len() + 1];
        dp[0] = true;
        for (j, &pc) in p.iter().enumerate() {
            if pc == b'%' {
                dp[j + 1] = dp[j];
            }
        }
        for &sc in s {
            let mut prev = dp[0];
            dp[0] = false;
            for (j, &pc) in p.iter().enumerate() {
                let next = match pc {
                    b'%' => dp[j + 1] || dp[j],
                    b'_' => prev,
                    c => prev && c == sc,
                };
                prev = dp[j + 1];
                dp[j + 1] = next;
            }
        }
        *dp.last().unwrap_or(&false)
    }
}

impl FallibleIterator for Filter {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        while let Some(tuple) = self.child.next()? {
            if self.eval(&tuple) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

impl Executor for Filter {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.child.rewind()
    }
}

#[derive(Debug)]
pub struct Project {
    child: Box<PlanNode>,
    col_indices: Vec<usize>,
    output_schema: TupleSchema,
}

impl Project {
    pub fn new(
        child: Box<PlanNode>,
        col_ids: Vec<primitives::ColumnId>,
    ) -> Result<Self, ExecutionError> {
        let col_indices: Vec<usize> = col_ids.iter().map(|&c| usize::from(c)).collect();
        let output_schema = child
            .schema()
            .project(&col_indices)
            .map_err(|e| ExecutionError::TypeError(e.to_string()))?;
        Ok(Self {
            child,
            col_indices,
            output_schema,
        })
    }
}

impl FallibleIterator for Project {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        match self.child.next()? {
            Some(tuple) => Ok(Some(tuple.project(&self.col_indices))),
            None => Ok(None),
        }
    }
}

impl Executor for Project {
    fn schema(&self) -> &TupleSchema {
        &self.output_schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.child.rewind()
    }
}

#[derive(Debug)]
pub struct Sort {
    ascending: bool,
    col_id: primitives::ColumnId,
    child: Box<PlanNode>,
    sorted: Vec<Tuple>,
    cursor: usize,
    materialized: bool,
}

impl Sort {
    pub fn new(ascending: bool, col_id: primitives::ColumnId, child: Box<PlanNode>) -> Self {
        Self {
            ascending,
            col_id,
            child,
            sorted: Vec::new(),
            cursor: 0,
            materialized: false,
        }
    }

    fn materialize_tuples(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        while let Some(tuple) = self.child.next()? {
            self.sorted.push(tuple);
        }

        let col_index = usize::from(self.col_id);
        self.sorted.sort_by(|a, b| {
            let ord = match (a.get(col_index), b.get(col_index)) {
                (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            if self.ascending { ord } else { ord.reverse() }
        });

        self.materialized = true;
        Ok(())
    }
}

impl FallibleIterator for Sort {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.materialize_tuples()?;
        if self.cursor >= self.sorted.len() {
            return Ok(None);
        }
        let tuple = self.sorted[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(tuple))
    }
}

impl Executor for Sort {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.cursor = 0;
        Ok(())
    }
}

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct Limit {
    limit: u64,
    offset: u64,
    count: u64,
    child: Box<PlanNode>,
    initialized: bool,
}

impl Limit {
    fn skip_offset(&mut self) -> Result<(), ExecutionError> {
        if self.initialized {
            return Ok(());
        }
        for _ in 0..self.offset {
            let Some(_) = self.child.next()? else {
                break;
            };
        }
        self.initialized = true;
        Ok(())
    }
}

impl FallibleIterator for Limit {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        self.skip_offset()?;
        if self.count >= self.limit {
            return Ok(None);
        }

        let ch = self.child.next()?;
        match ch {
            Some(tup) => {
                self.count += 1;
                Ok(Some(tup))
            }
            None => Ok(None),
        }
    }
}

impl Executor for Limit {
    fn schema(&self) -> &TupleSchema {
        self.child.schema()
    }
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.skip_offset()
    }
}
