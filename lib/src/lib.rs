use std::marker::PhantomData;

use parquet::{
    file::{
        reader::ChunkReader,
        serialized_reader::{ReadOptions, SerializedFileReader},
        writer::SerializedFileWriter,
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

pub trait Schema: Sized {
    type SortColumn;

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

    fn write<W: std::io::Write + Send, I: IntoIterator<Item = Vec<Self>>>(
        writer: W,
        properties: parquet::file::properties::WriterProperties,
        groups: I,
    ) -> Result<parquet::format::FileMetaData, Error>;

    fn write_group<W: std::io::Write + Send>(
        file_writer: &mut SerializedFileWriter<W>,
        groups: &[Self],
    ) -> Result<parquet::file::metadata::RowGroupMetaDataPtr, Error>;
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
