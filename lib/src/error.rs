use parquet::schema::types::ColumnPath;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Field error")]
    InvalidField(String),
    #[error("Oversized row value error")]
    OversizedRowValue { row_group_index: usize },
}

#[derive(thiserror::Error, Debug)]
pub enum ValueError {
    #[error("String field value contains null byte")]
    NullByteString {
        column_path: ColumnPath,
        index: usize,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum SortKeyError {
    #[error("Non-singleton byte array sort key")]
    NonSingletonByteArrayKey,
    #[error("Unsupported sort key length")]
    UnsupportedLength(usize),
}
