use fallible_iterator::FallibleIterator;

use crate::tuple::{Tuple, TupleSchema};

use super::{ExecutionError, Executor};

#[derive(Debug)]
pub struct Aggregate;

impl FallibleIterator for Aggregate {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for Aggregate {
    fn schema(&self) -> &TupleSchema { todo!() }
}
