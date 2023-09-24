#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Field error")]
    InvalidField(String),
}

#[derive(thiserror::Error, Debug)]
pub enum SortKeyError {
    #[error("Non-singleton byte array sort key")]
    NonSingletonByteArrayKey,
    #[error("Unsupported sort key length")]
    UnsupportedLength(usize),
}
