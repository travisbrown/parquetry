use parquet::{
    basic::{LogicalType, Type as PhysicalType},
    format::TimeUnit,
};

use super::error::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DateTimeUnit {
    Millis,
    Micros,
}

/// Mapping between Rust and Parquet types, with conversion code, etc.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypeMapping {
    Bool,
    I32,
    I64,
    U32,
    U64,
    String,
    DateTime(DateTimeUnit),
    F32,
    F64,
    ByteArray,
    FixedLengthByteArray(usize),
}

impl TypeMapping {
    pub fn from_types(
        logical_type: Option<LogicalType>,
        physical_type: PhysicalType,
        type_length: i32,
    ) -> Result<Self, Error> {
        match logical_type {
            None => match physical_type {
                PhysicalType::BOOLEAN => Ok(Self::Bool),
                PhysicalType::INT32 => Ok(Self::I32),
                PhysicalType::INT64 => Ok(Self::I64),
                PhysicalType::FLOAT => Ok(Self::F32),
                PhysicalType::DOUBLE => Ok(Self::F64),
                PhysicalType::BYTE_ARRAY => Ok(Self::ByteArray),
                PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                    Ok(Self::FixedLengthByteArray(type_length as usize))
                }
                PhysicalType::INT96 => Err(Error::UnsupportedPhysicalType(PhysicalType::INT96)),
            },
            Some(LogicalType::String) => Ok(Self::String),
            Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            }) => Ok(Self::U32),
            Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            }) => Ok(Self::U64),
            Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: true,
            }) => Ok(Self::I32),
            Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: true,
            }) => Ok(Self::I64),
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::MILLIS(_),
            }) => Ok(Self::DateTime(DateTimeUnit::Millis)),
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::MICROS(_),
            }) => Ok(Self::DateTime(DateTimeUnit::Micros)),
            Some(other) => Err(Error::UnsupportedLogicalType(other)),
        }
    }

    pub fn rust_type_name(&self) -> String {
        match self {
            Self::Bool => "bool".to_string(),
            Self::I32 => "i32".to_string(),
            Self::I64 => "i64".to_string(),
            Self::U32 => "u32".to_string(),
            Self::U64 => "u64".to_string(),
            Self::String => "String".to_string(),
            Self::DateTime(_) => "chrono::DateTime<chrono::Utc>".to_string(),
            Self::F32 => "f32".to_string(),
            Self::F64 => "f64".to_string(),
            Self::ByteArray => "Vec<u8>".to_string(),
            Self::FixedLengthByteArray(len) => format!("[u8; {}]", len),
        }
    }

    pub fn physical_type_conversion(&self, name: &str) -> String {
        match self {
            Self::Bool | Self::I32 | Self::I64 | Self::F32 | Self::F64 => format!("*{}", name),
            Self::U32 => format!("*{} as i32", name),
            Self::U64 => format!("*{} as i64", name),
            Self::String => format!("{}.as_str().into()", name),
            Self::DateTime(DateTimeUnit::Millis) => format!("{}.timestamp_millis()", name),
            Self::DateTime(DateTimeUnit::Micros) => format!("{}.timestamp_micros()", name),
            Self::ByteArray => format!("{}.as_slice().into()", name),
            Self::FixedLengthByteArray(_) => format!("{}.to_vec().into()", name),
        }
    }

    pub fn row_field_variant(&self) -> &'static str {
        match self {
            Self::Bool => "Bool",
            Self::I32 => "Int",
            Self::I64 => "Long",
            Self::U32 => "UInt",
            Self::U64 => "ULong",
            Self::String => "Str",
            Self::DateTime(DateTimeUnit::Millis) => "TimestampMillis",
            Self::DateTime(DateTimeUnit::Micros) => "TimestampMicros",
            Self::F32 => "Float",
            Self::F64 => "Double",
            Self::ByteArray | Self::FixedLengthByteArray(_) => "Bytes",
        }
    }

    pub fn row_field_conversion(&self, name: &str) -> String {
        match self {
            Self::Bool | Self::I32 | Self::I64 | Self::U32 | Self::U64 | Self::F32 | Self::F64 => {
                format!("*{}", name)
            }
            Self::String => format!("{}.clone()", name),
            Self::DateTime(date_time_unit) => {
                let method = match date_time_unit {
                    DateTimeUnit::Millis => "timestamp_millis_opt",
                    DateTimeUnit::Micros => "timestamp_micros",
                };
                let error_handling = format!(".ok_or_else(|| {})?", Self::error(name));
                format!(
                    "chrono::TimeZone::{}(&chrono::Utc, *{}).single(){}",
                    method, name, error_handling
                )
            }
            Self::ByteArray => format!("{}.data().to_vec()", name),
            Self::FixedLengthByteArray(_) => format!(
                "{}.data().try_into().map_err(|_| {})?",
                name,
                Self::error(name)
            ),
        }
    }

    pub fn disallowed_derives(&self) -> Vec<&'static str> {
        match self {
            Self::String | Self::ByteArray => vec!["Copy"],
            Self::F32 | Self::F64 => vec!["Eq"],
            _ => vec![],
        }
    }

    fn error(name: &str) -> String {
        format!(
            "parquetry::error::Error::InvalidField(\"{}\".to_string())",
            name
        )
    }
}
