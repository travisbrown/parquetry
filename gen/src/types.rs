use super::error::Error;
use parquet::{
    basic::{LogicalType, Type as PhysicalType},
    format::TimeUnit,
};

const EPOCH_DATE: &str = "chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()";

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
    Date,
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
            Some(LogicalType::Date) => Ok(Self::Date),
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

    pub fn attributes(&self, serde_support: bool, optional: bool) -> Option<String> {
        if serde_support {
            match self {
                Self::DateTime(DateTimeUnit::Millis) => Some(if optional {
                    "#[serde(with = \"chrono::serde::ts_milliseconds_option\")]".to_string()
                } else {
                    "#[serde(with = \"chrono::serde::ts_milliseconds\")]".to_string()
                }),
                Self::DateTime(DateTimeUnit::Micros) => Some(if optional {
                    "#[serde(with = \"chrono::serde::ts_microseconds_option\")]".to_string()
                } else {
                    "#[serde(with = \"chrono::serde::ts_microseconds\")]".to_string()
                }),
                _ => None,
            }
        } else {
            None
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
            Self::Date => "chrono::NaiveDate".to_string(),
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
            Self::Date => format!(
                "{}.signed_duration_since({}).num_days() as i32",
                name, EPOCH_DATE
            ),
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
            Self::Date => "Date",
            Self::DateTime(DateTimeUnit::Millis) => "TimestampMillis",
            Self::DateTime(DateTimeUnit::Micros) => "TimestampMicros",
            Self::F32 => "Float",
            Self::F64 => "Double",
            Self::ByteArray | Self::FixedLengthByteArray(_) => "Bytes",
        }
    }

    pub fn row_field_conversion(&self, field_name: &str, binding_name: &str) -> String {
        match self {
            Self::Bool | Self::I32 | Self::I64 | Self::U32 | Self::U64 | Self::F32 | Self::F64 => {
                format!("*{}", binding_name)
            }
            Self::String => format!("{}.clone()", binding_name),
            Self::Date => {
                let delta = format!("chrono::TimeDelta::try_days(*{} as i64)", binding_name);
                let error_handling = format!(".ok_or_else(|| {})?", Self::error(field_name));

                format!(
                    "{}.and_then(|delta| {}.checked_add_signed(delta)){}",
                    delta, EPOCH_DATE, error_handling
                )
            }
            Self::DateTime(date_time_unit) => {
                let method = match date_time_unit {
                    DateTimeUnit::Millis => "timestamp_millis_opt",
                    DateTimeUnit::Micros => "timestamp_micros",
                };
                let error_handling = format!(".ok_or_else(|| {})?", Self::error(field_name));
                format!(
                    "chrono::TimeZone::{}(&chrono::Utc, *{}).single(){}",
                    method, binding_name, error_handling
                )
            }
            Self::ByteArray => format!("{}.data().to_vec()", binding_name),
            Self::FixedLengthByteArray(_) => format!(
                "{}.data().try_into().map_err(|_| {})?",
                binding_name,
                Self::error(field_name)
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

    pub fn is_copy(&self) -> bool {
        matches!(
            self,
            Self::Bool
                | Self::I32
                | Self::I64
                | Self::U32
                | Self::U64
                | Self::F32
                | Self::F64
                | Self::DateTime(_)
                | Self::FixedLengthByteArray(_)
        )
    }

    pub fn write_bytes(&self) -> String {
        let mut code = String::new();
        match self {
            Self::Bool => {
                code.push_str("bytes.push(if column.descending { if value { 0 } else { 1 } } else { if value { 1 } else { 0 } });");
            }
            Self::I32 | Self::I64 | Self::U32 | Self::U64 | Self::F32 | Self::F64 => {
                code.push_str("for b in value.to_be_bytes() {");
                code.push_str("bytes.push(if column.descending { !b } else { b });");
                code.push('}');
            }
            Self::Date => {
                code.push_str(&format!(
                    "for b in (value.signed_duration_since({}).num_days() as i32).to_be_bytes() {{",
                    EPOCH_DATE
                ));
                code.push_str("bytes.push(if column.descending { !b } else { b });");
                code.push('}');
            }
            Self::DateTime(_) => {
                code.push_str("for b in value.timestamp_micros().to_be_bytes() {");
                code.push_str("bytes.push(if column.descending { !b } else { b });");
                code.push('}');
            }
            Self::String => {
                code.push_str("for b in value.as_bytes() {");
                code.push_str("bytes.push(if column.descending { !b } else { *b });");
                code.push('}');
                code.push_str("bytes.push(if column.descending { u8::MAX } else { b'\\0' });");
            }
            Self::ByteArray => {
                code.push_str("for b in value {");
                code.push_str("bytes.push(if column.descending { !b } else { *b });");
                code.push('}');
            }
            Self::FixedLengthByteArray(_) => {
                code.push_str("for b in value {");
                code.push_str("bytes.push(if column.descending { !b } else { b });");
                code.push('}');
            }
        }

        code
    }

    fn error(name: &str) -> String {
        format!(
            "parquetry::error::Error::InvalidField(\"{}\".to_string())",
            name
        )
    }
}
