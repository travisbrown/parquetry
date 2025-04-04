use crate::error::Error;
use std::{iter::Peekable, marker::PhantomData};

pub struct WriteConfig<T, S, F: Fn(&T) -> S> {
    pub max_size: S,
    pub get_size: F,
    pub fail_on_oversized: bool,
    _item: PhantomData<T>,
}

impl<T, S: Copy, F: Fn(&T) -> S + Copy> WriteConfig<T, S, F> {
    pub fn size_counter(&self) -> SizeCounter<T, S, F> {
        SizeCounter::new(self.max_size, self.get_size)
    }
}

pub struct SizeCounter<T, S, F: Fn(&T) -> S> {
    max_size: S,
    get_size: F,
    current_size: Option<S>,
    oversized: Option<S>,
    _item: PhantomData<T>,
}

impl<T, S, F: Fn(&T) -> S> SizeCounter<T, S, F> {
    pub fn new(max_size: S, get_size: F) -> Self {
        Self {
            max_size,
            get_size,
            current_size: None,
            oversized: None,
            _item: PhantomData,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.current_size.is_none()
    }

    pub fn reset(&mut self) {
        self.current_size = None;
        self.oversized = None;
    }
}

impl<T, S: Copy, F: Fn(&T) -> S> SizeCounter<T, S, F> {
    pub fn checked(&mut self, item: T) -> SizeChecked<T, S> {
        if let Some(size) = self.oversized.take() {
            SizeChecked::Oversized {
                value: item,
                size,
                limit: self.max_size,
            }
        } else {
            SizeChecked::Valid(item)
        }
    }
}

impl<T, S: Copy + std::ops::Add<Output = S> + PartialOrd, F: Fn(&T) -> S> SizeCounter<T, S, F> {
    pub fn add(&mut self, item: &T) -> bool {
        let next_size = (self.get_size)(item);

        match self.current_size {
            Some(current_size) => {
                let new_current_size = next_size + current_size;
                if new_current_size <= self.max_size {
                    self.current_size = Some(new_current_size);
                    true
                } else {
                    false
                }
            }
            None => {
                if next_size > self.max_size {
                    self.oversized = Some(next_size);
                }
                self.current_size = Some(next_size);

                true
            }
        }
    }
}

pub trait SchemaWrite<T, W: std::io::Write> {
    fn write_row_group<'a, E: From<Error>, I: Iterator<Item = Result<&'a T, E>>>(
        &mut self,
        values: &mut I,
    ) -> Result<parquet::file::metadata::RowGroupMetaDataPtr, E>
    where
        T: 'a;

    fn write_item(&mut self, value: &T) -> Result<(), Error>;
    fn finish_row_group(&mut self) -> Result<parquet::file::metadata::RowGroupMetaDataPtr, Error>;

    fn finish(self) -> Result<parquet::format::FileMetaData, Error>;
}

/// Represents a value to be written that may exceed a size limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd)]
pub enum SizeChecked<T, S> {
    Valid(T),
    Oversized { value: T, size: S, limit: S },
}

impl<T, S> SizeChecked<T, S> {
    pub fn value(&self) -> &T {
        match self {
            Self::Valid(value) => value,
            Self::Oversized { value, .. } => value,
        }
    }
}

impl<T, S> From<SizeChecked<T, S>> for Result<T, T> {
    fn from(value: SizeChecked<T, S>) -> Self {
        match value {
            SizeChecked::Valid(value) => Ok(value),
            SizeChecked::Oversized { value, .. } => Err(value),
        }
    }
}

pub(super) struct RowGroupSplitter<T, S, E, I: Iterator<Item = Result<T, E>>, F: Fn(&T) -> S> {
    underlying: Peekable<I>,
    size_counter: SizeCounter<T, S, F>,
}

impl<T, S, E: From<Error>, I: Iterator<Item = Result<T, E>>, F: Fn(&T) -> S>
    RowGroupSplitter<T, S, E, I, F>
{
    pub(super) fn new(underlying: I, max_size: S, get_size: F) -> Self {
        Self {
            underlying: underlying.peekable(),
            size_counter: SizeCounter::new(max_size, get_size),
        }
    }

    pub(super) fn reset(&mut self) -> bool {
        self.size_counter.reset();

        self.underlying.peek().is_some()
    }
}

impl<
        T,
        S: Copy + std::ops::Add<Output = S> + PartialOrd,
        E,
        I: Iterator<Item = Result<T, E>>,
        F: Fn(&T) -> S,
    > Iterator for RowGroupSplitter<T, S, E, I, F>
{
    type Item = Result<SizeChecked<T, S>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.underlying.peek().is_none_or(|item| {
            item.as_ref()
                .map_or(true, |next_item| self.size_counter.add(next_item))
        }) {
            self.underlying
                .next()
                .map(|result| result.map(|item| self.size_counter.checked(item)))
        } else {
            None
        }
    }
}
