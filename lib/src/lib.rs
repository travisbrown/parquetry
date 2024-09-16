use std::iter::Peekable;
use std::marker::PhantomData;

use parquet::{
    basic::LogicalType,
    file::{
        reader::ChunkReader,
        serialized_reader::{ReadOptions, SerializedFileReader},
    },
    format::SortingColumn,
    record::{reader::RowIter, Row},
    schema::types::{ColumnPath, SchemaDescPtr},
};

pub mod error;

use crate::error::Error;

pub struct ColumnInfo {
    pub index: usize,
    pub path: &'static [&'static str],
}

impl ColumnInfo {
    pub fn path(&self) -> ColumnPath {
        ColumnPath::new(self.path.iter().map(|part| part.to_string()).collect())
    }

    pub fn sorting(&self) -> SortingColumn {
        SortingColumn::new(self.index as i32, false, false)
    }
}

pub trait SortColumn {
    fn index(&self) -> usize;
}

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Sort<C> {
    pub column: C,
    pub descending: bool,
    pub nulls_first: bool,
}

impl<C: Copy> Sort<C> {
    pub fn new(column: C) -> Self {
        Self {
            column,
            descending: false,
            nulls_first: false,
        }
    }

    pub fn descending(&self) -> Self {
        Self {
            column: self.column,
            descending: true,
            nulls_first: self.nulls_first,
        }
    }

    pub fn nulls_first(&self) -> Self {
        Self {
            column: self.column,
            descending: self.descending,
            nulls_first: true,
        }
    }

    pub fn sorting_column(&self) -> SortingColumn
    where
        C: SortColumn,
    {
        SortingColumn::new(
            self.column.index() as i32,
            self.descending,
            self.nulls_first,
        )
    }
}

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SortKey<C> {
    Columns1(Sort<C>),
    Columns2(Sort<C>, Sort<C>),
    Columns3(Sort<C>, Sort<C>, Sort<C>),
    Columns4(Sort<C>, Sort<C>, Sort<C>, Sort<C>),
    Columns5(Sort<C>, Sort<C>, Sort<C>, Sort<C>, Sort<C>),
}

impl<C: Copy> SortKey<C> {
    pub fn columns(&self) -> Vec<Sort<C>> {
        match self {
            Self::Columns1(column_0) => vec![*column_0],
            Self::Columns2(column_0, column_1) => vec![*column_0, *column_1],
            Self::Columns3(column_0, column_1, column_2) => vec![*column_0, *column_1, *column_2],
            Self::Columns4(column_0, column_1, column_2, column_3) => {
                vec![*column_0, *column_1, *column_2, *column_3]
            }
            Self::Columns5(column_0, column_1, column_2, column_3, column_4) => {
                vec![*column_0, *column_1, *column_2, *column_3, *column_4]
            }
        }
    }
}

impl<C: Copy + SortColumn> From<SortKey<C>> for Vec<SortingColumn> {
    fn from(value: SortKey<C>) -> Self {
        value
            .columns()
            .iter()
            .map(|sort| sort.sorting_column())
            .collect()
    }
}

pub trait Schema: Sized {
    type SortColumn;
    type Writer<W: std::io::Write + Send>: SchemaWrite<Self, W>;

    fn source() -> &'static str;
    fn schema() -> SchemaDescPtr;

    fn sort_key(
        columns: &[Sort<Self::SortColumn>],
    ) -> Result<SortKey<Self::SortColumn>, error::SortKeyError>
    where
        Self::SortColumn: SortColumn + Copy,
    {
        if columns.len() > 5 {
            Err(error::SortKeyError::UnsupportedLength(columns.len()))
        } else {
            let schema = Self::schema();
            let descriptors = schema.columns();

            if columns.iter().any(|column| {
                descriptors[column.column.index()].physical_type()
                    == parquet::basic::Type::BYTE_ARRAY
                    && descriptors[column.column.index()].logical_type()
                        != Some(LogicalType::String)
            }) {
                Err(error::SortKeyError::NonSingletonByteArrayKey)
            } else {
                match columns.len() {
                    1 => Ok(SortKey::Columns1(columns[0])),
                    2 => Ok(SortKey::Columns2(columns[0], columns[1])),
                    3 => Ok(SortKey::Columns3(columns[0], columns[1], columns[2])),
                    4 => Ok(SortKey::Columns4(
                        columns[0], columns[1], columns[2], columns[3],
                    )),
                    5 => Ok(SortKey::Columns5(
                        columns[0], columns[1], columns[2], columns[3], columns[4],
                    )),
                    other => Err(error::SortKeyError::UnsupportedLength(other)),
                }
            }
        }
    }

    fn sort_key_value(&self, sort_key: SortKey<Self::SortColumn>) -> Vec<u8>;

    fn read<R: ChunkReader + 'static>(reader: R, options: ReadOptions) -> SchemaIter<Self> {
        match SerializedFileReader::new_with_options(reader, options) {
            Ok(file_reader) => SchemaIter::Streaming {
                rows: RowIter::from_file_into(Box::new(file_reader)),
                _item: PhantomData,
            },
            Err(error) => SchemaIter::Failed(Some(Error::from(error))),
        }
    }

    fn writer<W: std::io::Write + Send>(
        writer: W,
        properties: parquet::file::properties::WriterProperties,
    ) -> Result<Self::Writer<W>, Error>;

    fn write_row_groups<W: std::io::Write + Send, I: IntoIterator<Item = Vec<Self>>>(
        writer: W,
        properties: parquet::file::properties::WriterProperties,
        groups: I,
    ) -> Result<parquet::format::FileMetaData, Error> {
        let mut writer = Self::writer(writer, properties)?;

        for group in groups {
            writer.write_row_group::<Error, _>(&mut group.iter().map(Ok))?;
        }
        writer.finish()
    }

    fn write<
        W: std::io::Write + Send,
        E: From<Error>,
        I: Iterator<Item = Result<Self, E>>,
        S: Copy + std::ops::Add<Output = S> + PartialOrd,
        F: Fn(&Self) -> S,
    >(
        writer: W,
        properties: parquet::file::properties::WriterProperties,
        max_size: S,
        get_size: F,
        fail_on_oversized: bool,
        items: I,
    ) -> Result<parquet::format::FileMetaData, E> {
        let mut writer = Self::writer(writer, properties)?;
        let mut row_group_splitter = RowGroupSplitter::new(items, max_size, get_size);
        let mut row_group_index = 0;

        while row_group_splitter.reset() {
            if fail_on_oversized {
                for result in row_group_splitter.by_ref() {
                    match result {
                        Ok(SizeChecked::Valid(value)) => writer.write_item(&value).map_err(E::from),
                        Ok(SizeChecked::Oversized { .. }) => {
                            Err(E::from(Error::OversizedRowValue {
                                row_group_index: Some(row_group_index),
                            }))
                        }
                        Err(error) => Err(error),
                    }?;
                }
            } else {
                for result in row_group_splitter.by_ref() {
                    match result {
                        Ok(size_checked) => {
                            writer.write_item(size_checked.value()).map_err(E::from)
                        }
                        Err(error) => Err(error),
                    }?;
                }
            }
            writer.finish_row_group()?;

            row_group_index += 1;
        }

        writer.finish().map_err(E::from)
    }
}

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
                .map(|row| row.map_err(Error::from).and_then(|row| row.try_into())),
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

pub struct RowGrouper<T, S, F> {
    max_size: S,
    get_size: F,
    _item: PhantomData<T>,
}

/*impl<T, F: Fn(&T) -> usize> RowGrouper<T, usize, F> {
    pub fn by_count(count: usize) -> Self {
        Self {
            max_size: count,
            get_size: |_| 1,
            _item: PhantomData,
        }
    }
}*/

/// Represents a value to be written that may exceed a size limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd)]
pub enum SizeChecked<T, S> {
    Valid(T),
    Oversized { value: T, size: S, limit: S },
}

impl<T, S> SizeChecked<T, S> {
    fn value(&self) -> &T {
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

struct RowGroupSplitter<T, S, E, I: Iterator<Item = Result<T, E>>, F: Fn(&T) -> S> {
    underlying: Peekable<I>,
    max_size: S,
    get_size: F,
    current_size: Option<S>,
}

impl<T, S, E: From<Error>, I: Iterator<Item = Result<T, E>>, F: Fn(&T) -> S>
    RowGroupSplitter<T, S, E, I, F>
{
    fn new(underlying: I, max_size: S, get_size: F) -> Self {
        Self {
            underlying: underlying.peekable(),
            max_size,
            get_size,
            current_size: None,
        }
    }

    fn reset(&mut self) -> bool {
        self.current_size = None;

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
        let mut oversized = None;

        if self.underlying.peek().map_or(true, |item| {
            item.as_ref().map_or(true, |next_item| {
                let next_size = (self.get_size)(next_item);

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
                            oversized = Some(next_size);
                        }
                        self.current_size = Some(next_size);

                        true
                    }
                }
            })
        }) {
            self.underlying.next().map(|result| {
                result.map(|item| {
                    if let Some(size) = oversized {
                        SizeChecked::Oversized {
                            value: item,
                            size,
                            limit: self.max_size,
                        }
                    } else {
                        SizeChecked::Valid(item)
                    }
                })
            })
        } else {
            None
        }
    }
}
