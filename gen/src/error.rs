use std::path::PathBuf;

use parquet::{
    basic::{LogicalType, Type as PhysicalType},
    schema::types::Type,
};

use super::schema::GenField;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Invalid schema path")]
    InvalidPath(PathBuf),
    #[error("Syntax error")]
    Syntax(#[from] syn::Error),
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Invalid root schema")]
    InvalidRootSchema(Type),
    #[error("Unsupported logical type")]
    UnsupportedLogicalType(LogicalType),
    #[error("Unsupported physical type")]
    UnsupportedPhysicalType(PhysicalType),
    #[error("Unsupported repetition shape")]
    UnsupportedRepetition(String),
    #[error("Unsupported field type")]
    UnsupportedField(GenField),
}
