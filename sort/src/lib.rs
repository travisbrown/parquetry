use bincode::serde::Compat;
use parquetry::{
    sort::{SortColumn, SortKey},
    write::{SchemaWrite, SizeChecked, SizeCounter},
    Schema,
};
use rocksdb::{IteratorMode, MergeOperands, Options, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RocksDb error")]
    Db(#[from] rocksdb::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Bincode encoding error")]
    BincodeEncoding(#[from] bincode::error::EncodeError),
    #[error("Bincode decoding error")]
    BincodeDecoding(#[from] bincode::error::DecodeError),
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Parquetry error")]
    Parquetry(#[from] parquetry::error::Error),
    #[error("Invalid value")]
    InvalidValue(Vec<u8>),
    #[error("Row group max size is too small")]
    InvalidRowGroupSize(usize),
}

#[derive(Clone)]
pub struct SortDb<A: Schema + DeserializeOwned + Serialize> {
    db: Arc<DB>,
    sort_key: SortKey<A::SortColumn>,
}

impl<A: Schema + DeserializeOwned + Serialize> SortDb<A> {
    pub fn open<P: AsRef<Path>>(path: P, sort_key: SortKey<A::SortColumn>) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);

        Self::open_opt(path, sort_key, options)
    }

    pub fn open_opt<P: AsRef<Path>>(
        path: P,
        sort_key: SortKey<A::SortColumn>,
        mut options: Options,
    ) -> Result<Self, Error> {
        options.set_merge_operator_associative("concatenation", concatenation_merge);

        let db = Arc::new(DB::open(&options, path)?);

        Ok(Self { db, sort_key })
    }

    pub fn insert(&self, value: &A) -> Result<(), Error>
    where
        A::SortColumn: Copy,
    {
        let key = value.sort_key_value(self.sort_key);
        let bytes = bincode::encode_to_vec(Compat(value), bincode::config::standard())?;

        self.db.merge(key, bytes)?;

        Ok(())
    }

    pub fn write<
        W: std::io::Write + Send,
        S: Copy + std::ops::Add<Output = S> + PartialOrd,
        F: Fn(&A) -> S,
    >(
        &self,
        writer: W,
        properties: parquet::file::properties::WriterPropertiesBuilder,
        max_size: S,
        get_size: F,
        fail_on_oversized: bool,
    ) -> Result<parquet::format::FileMetaData, Error>
    where
        A::SortColumn: Copy + SortColumn,
    {
        let properties = properties
            .set_sorting_columns(Some(self.sort_key.into()))
            .build();

        let mut writer = A::writer(writer, properties)?;
        let mut size_counter = SizeCounter::new(max_size, get_size);
        let mut row_group_index = 0;

        for result in self.db.iterator(IteratorMode::Start) {
            let (_, value_bytes) = result?;
            let mut current = 0;

            while current + 4 < value_bytes.len() {
                let len = u32::from_be_bytes(
                    value_bytes[current..current + 4]
                        .try_into()
                        .map_err(|_| Error::InvalidValue(value_bytes.to_vec()))?,
                ) as usize;

                current += 4;

                let (bincode::serde::Compat(item), _) = bincode::decode_from_slice::<Compat<A>, _>(
                    &value_bytes[current..current + len],
                    bincode::config::standard(),
                )?;

                current += len;

                loop {
                    if size_counter.add(&item) {
                        match size_counter.checked(item) {
                            SizeChecked::Valid(value) => writer.write_item(&value),
                            SizeChecked::Oversized { value, .. } => {
                                if fail_on_oversized {
                                    Err(parquetry::error::Error::OversizedRowValue {
                                        row_group_index,
                                    })
                                } else {
                                    writer.write_item(&value)
                                }
                            }
                        }?;

                        break;
                    } else {
                        writer.finish_row_group()?;
                        size_counter.reset();

                        row_group_index += 1;
                    }
                }
            }
        }

        if !size_counter.is_empty() {
            writer.finish_row_group()?;
        }

        Ok(writer.finish()?)
    }

    pub fn write_file<
        P: AsRef<Path>,
        S: Copy + std::ops::Add<Output = S> + PartialOrd,
        F: Fn(&A) -> S,
    >(
        &self,
        output: P,
        properties: parquet::file::properties::WriterPropertiesBuilder,
        max_size: S,
        get_size: F,
        fail_on_oversized: bool,
    ) -> Result<parquet::format::FileMetaData, Error>
    where
        A::SortColumn: Copy + SortColumn,
    {
        let file = File::create(output)?;

        self.write(file, properties, max_size, get_size, fail_on_oversized)
    }
}

fn concatenation_merge(
    _new_key: &[u8],
    existing_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(operands.len());

    if let Some(value) = existing_value {
        result.extend((value.len() as u32).to_be_bytes());
        result.extend(value);
    };

    for operand in operands {
        result.extend((operand.len() as u32).to_be_bytes());
        result.extend(operand);
    }

    Some(result)
}
