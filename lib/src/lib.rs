use parquet::{
    basic::LogicalType,
    file::{
        reader::ChunkReader,
        serialized_reader::{ReadOptions, SerializedFileReader},
    },
    format::SortingColumn,
    record::reader::RowIter,
    schema::types::{ColumnPath, SchemaDescPtr},
};
use std::marker::PhantomData;

pub mod error;
pub mod read;
pub mod sort;
pub mod write;

use crate::error::Error;
use crate::sort::SortColumn;
use crate::write::SchemaWrite;

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
    type SortColumn;
    type Writer<W: std::io::Write + Send>: SchemaWrite<Self, W>;

    fn source() -> &'static str;
    fn schema() -> SchemaDescPtr;

    fn sort_key(
        columns: &[sort::Sort<Self::SortColumn>],
    ) -> Result<sort::SortKey<Self::SortColumn>, error::SortKeyError>
    where
        Self::SortColumn: sort::SortColumn + Copy,
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
                    1 => Ok(sort::SortKey::Columns1(columns[0])),
                    2 => Ok(sort::SortKey::Columns2(columns[0], columns[1])),
                    3 => Ok(sort::SortKey::Columns3(columns[0], columns[1], columns[2])),
                    4 => Ok(sort::SortKey::Columns4(
                        columns[0], columns[1], columns[2], columns[3],
                    )),
                    5 => Ok(sort::SortKey::Columns5(
                        columns[0], columns[1], columns[2], columns[3], columns[4],
                    )),
                    other => Err(error::SortKeyError::UnsupportedLength(other)),
                }
            }
        }
    }

    fn sort_key_value(&self, sort_key: sort::SortKey<Self::SortColumn>) -> Vec<u8>;

    fn read<R: ChunkReader + 'static>(reader: R, options: ReadOptions) -> read::SchemaIter<Self> {
        match SerializedFileReader::new_with_options(reader, options) {
            Ok(file_reader) => read::SchemaIter::Streaming {
                rows: RowIter::from_file_into(Box::new(file_reader)),
                _item: PhantomData,
            },
            Err(error) => read::SchemaIter::Failed(Some(Error::from(error))),
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
        let mut row_group_splitter = write::RowGroupSplitter::new(items, max_size, get_size);
        let mut row_group_index = 0;

        while row_group_splitter.reset() {
            if fail_on_oversized {
                for result in row_group_splitter.by_ref() {
                    match result {
                        Ok(write::SizeChecked::Valid(value)) => {
                            writer.write_item(&value).map_err(E::from)
                        }
                        Ok(write::SizeChecked::Oversized { .. }) => {
                            Err(E::from(Error::OversizedRowValue { row_group_index }))
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
