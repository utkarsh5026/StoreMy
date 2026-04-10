use fallible_iterator::FallibleIterator;

use crate::tuple::{Tuple, TupleSchema};

use super::{ExecutionError, Executor};

#[derive(Debug)]
pub struct NestedLoopJoin;

#[derive(Debug)]
pub struct HashJoin;

#[derive(Debug)]
pub struct SortMergeJoin;

impl FallibleIterator for NestedLoopJoin {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for NestedLoopJoin {
    fn schema(&self) -> &TupleSchema { todo!() }
    // no rewind override — uses default Err(RewindNotSupported)
}

impl FallibleIterator for HashJoin {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for HashJoin {
    fn schema(&self) -> &TupleSchema { todo!() }
    // no rewind override — uses default Err(RewindNotSupported)
}

impl FallibleIterator for SortMergeJoin {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for SortMergeJoin {
    fn schema(&self) -> &TupleSchema { todo!() }
    // no rewind override — uses default Err(RewindNotSupported)
}
