use fallible_iterator::FallibleIterator;

use crate::tuple::{Tuple, TupleSchema};

use super::{ExecutionError, Executor};

#[derive(Debug)]
pub struct SeqScan;

#[derive(Debug)]
pub struct IndexScan;

impl FallibleIterator for SeqScan {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for SeqScan {
    fn schema(&self) -> &TupleSchema { todo!() }
    fn rewind(&mut self) -> Result<(), ExecutionError> { todo!() }
}

impl FallibleIterator for IndexScan {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for IndexScan {
    fn schema(&self) -> &TupleSchema { todo!() }
    fn rewind(&mut self) -> Result<(), ExecutionError> { todo!() }
}
