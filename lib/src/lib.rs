use std::marker::PhantomData;

use parquet::{
    file::{
        reader::ChunkReader,
        serialized_reader::{ReadOptions, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    format::SortingColumn,
    record::{reader::RowIter, Row},
    schema::types::{ColumnPath, TypePtr},
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

pub trait Schema: Sized {
    fn source() -> &'static str;
    fn schema() -> TypePtr;

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
