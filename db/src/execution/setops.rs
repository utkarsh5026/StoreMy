use fallible_iterator::FallibleIterator;

use crate::tuple::{Tuple, TupleSchema};

use super::{ExecutionError, Executor};

#[derive(Debug)]
pub struct Union;

#[derive(Debug)]
pub struct Intersect;

#[derive(Debug)]
pub struct Except;

#[derive(Debug)]
pub struct Distinct;

impl FallibleIterator for Union {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for Union {
    fn schema(&self) -> &TupleSchema { todo!() }
}

impl FallibleIterator for Intersect {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for Intersect {
    fn schema(&self) -> &TupleSchema { todo!() }
}

impl FallibleIterator for Except {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for Except {
    fn schema(&self) -> &TupleSchema { todo!() }
}

impl FallibleIterator for Distinct {
    type Item = Tuple;
    type Error = ExecutionError;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> { todo!() }
}

impl Executor for Distinct {
    fn schema(&self) -> &TupleSchema { todo!() }
}
