#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Field error")]
    InvalidField(String),
}
