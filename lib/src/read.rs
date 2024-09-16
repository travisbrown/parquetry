use crate::error::Error;
use parquet::record::{reader::RowIter, Row};
use std::marker::PhantomData;

pub enum SchemaIter<T> {
    Failed(Option<Error>),
    Streaming {
        rows: RowIter<'static>,
        _item: PhantomData<T>,
    },
}

impl<T: TryFrom<Row, Error = Error>> Iterator for SchemaIter<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Failed(error) => error.take().map(|error| Err(error)),
            Self::Streaming { rows, .. } => rows
                .next()
                .map(|row| row.map_err(Error::from).and_then(T::try_from)),
        }
    }
}
