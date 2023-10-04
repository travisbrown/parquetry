use codegen::{Block, Module};

use crate::{
    schema::{GenColumn, GenSchema, GenType},
    types::{DateTimeUnit, TypeMapping},
};

use super::error::Error;

pub fn gen_test_code(test_module: &mut Module, schema: &GenSchema) -> Result<(), Error> {
    for gen_struct in schema.structs() {
        let arbitrary_fn = test_module
            .new_impl(&format!("super::{}", gen_struct.type_name))
            .impl_trait("quickcheck::Arbitrary")
            .new_fn("arbitrary")
            .arg("g", "&mut quickcheck::Gen")
            .ret("Self");

        arbitrary_fn.line("Self::new(");

        for gen_field in gen_struct.fields {
            arbitrary_fn.line(format!(
                "{},",
                arbitrary_value(&gen_field.gen_type, gen_field.optional)
            ));
        }

        arbitrary_fn.line(format!(
            ").expect(\"Invalid quickcheck::Arbitrary instance for {}\")",
            gen_struct.type_name
        ));
    }

    for line in gen_round_trip_write(&schema.type_name) {
        test_module.scope().raw(line);
    }

    for line in gen_round_trip_write_group(&schema.type_name) {
        test_module.scope().raw(line);
    }

    for line in gen_round_trip_serde_bincode(&schema.type_name) {
        test_module.scope().raw(line);
    }

    test_module
        .new_fn("gen_valid_timestamp_milli")
        .arg("g", "&mut quickcheck::Gen")
        .ret("i64")
        .push_block(gen_valid_timestamp_block("milli"));

    test_module
        .new_fn("gen_valid_timestamp_micro")
        .arg("g", "&mut quickcheck::Gen")
        .ret("i64")
        .push_block(gen_valid_timestamp_block("micro"));

    Ok(())
}

fn gen_valid_timestamp_block(date_time_unit: &str) -> Block {
    let mut block = Block::new("");
    block.line("use quickcheck::Arbitrary;");
    block.line(format!(
        "let min = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp_{}s();",
        date_time_unit
    ));
    block.line(format!(
        "let max = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_{}s();",
        date_time_unit
    ));
    block.line("let value: i64 = <_>::arbitrary(g);");
    block.line("if value < min { value % min } else if value > max { value % max } else { value }");
    block
}

fn gen_valid_string(optional: bool) -> String {
    let mut value = String::new();
    value.push('{');
    value.push_str("let mut value: String = quickcheck::Arbitrary::arbitrary(g);");
    value.push_str("value.retain(|char| char != '\0');");
    value.push_str("value");
    value.push('}');

    if optional {
        format!(
            "{{ let optional: Option<()> = <_>::arbitrary(g);\noptional.map(|_| {}) }}",
            value
        )
    } else {
        value
    }
}

const INVALID_ARBITRARY_INSTANCE_MESSAGE: &str =
    "Invalid quickcheck::Arbitrary instance for DateTime<Utc>";

fn gen_valid_date_time(date_time_unit: &str, optional: bool) -> String {
    let method_name = if date_time_unit == "milli" {
        "timestamp_millis_opt".to_string()
    } else {
        format!("timestamp_{}s", date_time_unit)
    };

    let digits = if date_time_unit == "milli" { 3 } else { 6 };

    let value = format!(
        "chrono::SubsecRound::trunc_subsecs(chrono::TimeZone::{}(&chrono::Utc, gen_valid_timestamp_{}(g)).single().expect(\"{}\"), {})",
        method_name,
        date_time_unit,
        INVALID_ARBITRARY_INSTANCE_MESSAGE,
        digits
    );

    if optional {
        format!(
            "{{ let optional: Option<()> = <_>::arbitrary(g);\noptional.map(|_| {}) }}",
            value
        )
    } else {
        value
    }
}

fn arbitrary_value(gen_type: &GenType, optional: bool) -> String {
    match gen_type {
        GenType::Column(GenColumn { mapping, .. }) => match mapping {
            TypeMapping::DateTime(date_time_unit) => {
                let date_time_unit = match date_time_unit {
                    DateTimeUnit::Millis => "milli",
                    DateTimeUnit::Micros => "micro",
                };
                gen_valid_date_time(date_time_unit, optional)
            }
            TypeMapping::FixedLengthByteArray(len) => {
                let values = (0..*len)
                    .map(|_| "u8::arbitrary(g)")
                    .collect::<Vec<_>>()
                    .join(", ");
                if optional {
                    format!("{{ let optional: Option<()> = <_>::arbitrary(g);\noptional.map(|_| [{}]) }}",  values)
                } else {
                    format!("[{}]", values)
                }
            }
            TypeMapping::F32 | TypeMapping::F64 => {
                if optional {
                    format!("match Option::<{}>::arbitrary(g) {{ Some(value) if value.is_nan() => Some(0.0), value => value }}", mapping.rust_type_name())
                } else {
                    format!("match {}::arbitrary(g) {{ value if value.is_nan() => 0.0, value => value }}", mapping.rust_type_name())
                }
            }
            TypeMapping::String => gen_valid_string(optional),
            _ => "<_>::arbitrary(g)".to_string(),
        },
        GenType::List { .. } => "<_>::arbitrary(g)".to_string(),
        GenType::Struct { .. } => "<_>::arbitrary(g)".to_string(),
    }
}

fn gen_round_trip_serde_bincode(type_name: &str) -> Vec<String> {
    vec![
        format!("fn round_trip_serde_bincode_impl(values: Vec<super::{}>) -> bool {{", type_name),
        format!("let wrapped = bincode::serde::Compat(&values);"),
        format!("let encoded = bincode::encode_to_vec(&wrapped, bincode::config::standard()).unwrap();"),
        format!("let decoded: (bincode::serde::Compat<Vec<super::{}>>, _) = bincode::decode_from_slice(&encoded.as_slice(), bincode::config::standard()).unwrap();", type_name),
        "decoded.0.0 == values".to_string(),
        "}".to_string(),
        "quickcheck::quickcheck! {".to_string(),
        format!("    fn round_trip_serde_bincode(values: Vec<super::{}>) -> bool {{", type_name),
        "        round_trip_serde_bincode_impl(values)".to_string(),
        "    }".to_string(),
        "}".to_string()
    ]
}

fn gen_round_trip_write(type_name: &str) -> Vec<String> {
    vec![
        format!("fn round_trip_write_impl(groups: Vec<Vec<super::{}>>) -> bool {{", type_name),
        format!("let test_dir = tempdir::TempDir::new(\"{}-data\").unwrap();", type_name),
        "let test_file_path = test_dir.path().join(\"write-data.parquet\");".to_string(),
        "let test_file = std::fs::File::create(&test_file_path).unwrap();".to_string(),
        format!("<super::{} as parquetry::Schema>::write(test_file, Default::default(), groups.clone()).unwrap();", type_name),
        "let read_file = std::fs::File::open(test_file_path).unwrap();".to_string(),
        "let read_options = parquet::file::serialized_reader::ReadOptionsBuilder::new().build();".to_string(),
        format!("let read_values = <super::{} as parquetry::Schema>::read(read_file, read_options).collect::<Result<Vec<_>, _>>().unwrap();", type_name),
        "read_values == groups.into_iter().flatten().collect::<Vec<_>>()".to_string(),
        "}".to_string(),
        "quickcheck::quickcheck! {".to_string(),
        format!("    fn round_trip_write(groups: Vec<Vec<super::{}>>) -> bool {{", type_name),
        "        round_trip_write_impl(groups)".to_string(),
        "    }".to_string(),
        "}".to_string()
    ]
}

fn gen_round_trip_write_group(type_name: &str) -> Vec<String> {
    vec![
        format!("fn round_trip_write_group_impl(groups: Vec<Vec<super::{}>>) -> bool {{", type_name),
        format!("let test_dir = tempdir::TempDir::new(\"{}-data\").unwrap();", type_name),
        "let test_file_path = test_dir.path().join(\"write_group-data.parquet\");".to_string(),
        "let test_file = std::fs::File::create(&test_file_path).unwrap();".to_string(),
        format!("let mut file_writer = parquet::file::writer::SerializedFileWriter::new(test_file, <super::{} as parquetry::Schema>::schema().root_schema_ptr(), Default::default()).unwrap();", type_name),
        format!("for group in &groups {{ <super::{} as parquetry::Schema>::write_group(&mut file_writer, group).unwrap(); }}", type_name),
        "file_writer.close().unwrap();".to_string(),
        "let read_file = std::fs::File::open(test_file_path).unwrap();".to_string(),
        "let read_options = parquet::file::serialized_reader::ReadOptionsBuilder::new().build();".to_string(),
        format!("let read_values = <super::{} as parquetry::Schema>::read(read_file, read_options).collect::<Result<Vec<_>, _>>().unwrap();", type_name),
        "read_values == groups.into_iter().flatten().collect::<Vec<_>>()".to_string(),
        "}".to_string(),
        "quickcheck::quickcheck! {".to_string(),
        format!("    fn round_trip_write_group(groups: Vec<Vec<super::{}>>) -> bool {{", type_name),
        "        round_trip_write_group_impl(groups)".to_string(),
        "    }".to_string(),
        "}".to_string()
    ]
}
