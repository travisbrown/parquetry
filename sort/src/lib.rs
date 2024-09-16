use bincode::serde::Compat;
use parquet::file::properties::WriterPropertiesBuilder;
use parquetry::{
    sort::{SortColumn, SortKey},
    Schema, SchemaWrite,
};
use rocksdb::{BlockBasedOptions, IteratorMode, MergeOperands, Options, DB};
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
        options.set_merge_operator_associative("concatenation", concatenation_merge);

        let mut block_options = BlockBasedOptions::default();
        block_options.set_ribbon_filter(10.0);

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

    pub fn write<P: AsRef<Path>>(
        &self,
        output: P,
        properties: WriterPropertiesBuilder,
        max_row_group_bytes: usize,
    ) -> Result<Vec<usize>, Error>
    where
        A::SortColumn: Copy + SortColumn,
    {
        let properties = properties
            .set_sorting_columns(Some(self.sort_key.into()))
            .build();

        let file = File::create(output)?;
        let mut writer = A::writer(file, properties)?;

        let mut current_bytes = 0;
        let mut group: Vec<A> = Vec::with_capacity(256);
        let mut group_counts = Vec::with_capacity(1);

        for result in self.db.iterator(IteratorMode::Start) {
            let bytes = result?.1;
            let mut current = 0;

            while current + 4 < bytes.len() {
                let len = u32::from_be_bytes(
                    bytes[current..current + 4]
                        .try_into()
                        .map_err(|_| Error::InvalidValue(bytes.to_vec()))?,
                ) as usize;

                current += 4;

                let decoded = bincode::decode_from_slice::<Compat<A>, _>(
                    &bytes[current..current + len],
                    bincode::config::standard(),
                )?
                .0;

                current += len;

                if current_bytes + len > max_row_group_bytes {
                    if group.is_empty() {
                        return Err(Error::InvalidRowGroupSize(max_row_group_bytes));
                    }

                    group_counts.push(group.len());
                    writer.write_row_group::<Error, _>(&mut group.iter().map(Ok))?;
                    group.clear();
                    current_bytes = 0;
                }

                current_bytes += len;
                group.push(decoded.0);
            }
        }

        if !group.is_empty() {
            group_counts.push(group.len());
            writer.write_row_group::<Error, _>(&mut group.iter().map(Ok))?;
        }

        writer.finish()?;

        Ok(group_counts)
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
