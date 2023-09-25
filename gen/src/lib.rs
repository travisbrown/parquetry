use codegen::Scope;
use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
use std::path::PathBuf;
use std::{path::Path, sync::Arc};

mod code;
mod column_code;
pub mod error;
pub mod schema;
mod test_code;
mod types;
mod util;

use error::Error;
use schema::{GenSchema, GenStruct};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub base_derives: Vec<&'static str>,
    pub format: bool,
    pub serde_support: bool,
    pub tests: bool,
}

impl Config {
    pub fn derives(&self) -> Vec<&'static str> {
        let mut derives = self.base_derives.clone();

        if self.serde_support {
            derives.push("serde::Deserialize");
            derives.push("serde::Serialize");
        }

        derives
    }
}

impl Default for Config {
    fn default() -> Self {
        let base_derives = vec!["Clone", "Copy", "Debug", "Eq", "PartialEq"];

        Self {
            base_derives,
            format: true,
            serde_support: true,
            tests: true,
        }
    }
}

#[derive(Debug)]
pub struct ParsedFileSchema {
    pub name: String,
    pub schema: GenSchema,
    pub descriptor: SchemaDescriptor,
    scope: Scope,
    absolute_path: PathBuf,
    config: Config,
}

impl ParsedFileSchema {
    pub fn code(&self) -> Result<String, Error> {
        let raw_code = self.scope.to_string();

        if self.config.format {
            let file = syn::parse_file(&format!(
                "#![cfg_attr(rustfmt, rustfmt_skip)]\n{}",
                raw_code
            ))?;
            Ok(prettyplease::unparse(&file))
        } else {
            Ok(raw_code)
        }
    }

    pub fn open<P: AsRef<Path>>(input: P, config: Config) -> Result<ParsedFileSchema, Error> {
        let input = input.as_ref();
        let schema_source = std::fs::read_to_string(input)?;
        let (schema, descriptor) = parse_schema(&schema_source, config.clone())?;
        let scope = schema_to_scope(&schema_source, &schema, &descriptor)?;

        let name = input
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(|file_name| file_name.split('.').next())
            .ok_or_else(|| Error::InvalidPath(input.to_path_buf()))?
            .to_string();

        Ok(ParsedFileSchema {
            name,
            schema,
            descriptor,
            scope,
            absolute_path: input.canonicalize()?,
            config,
        })
    }

    pub fn open_dir<P: AsRef<Path>>(
        input: P,
        config: Config,
        suffix: Option<&str>,
    ) -> Result<Vec<ParsedFileSchema>, Error> {
        let mut schemas = std::fs::read_dir(input)?
            .map(|result| result.map_err(Error::from).map(|entry| entry.path()))
            .filter_map(|result| {
                result.map_or_else(
                    |error| Some(Err(error)),
                    |path| {
                        if path.is_file() {
                            match path.file_name().and_then(|file_name| file_name.to_str()) {
                                Some(file_name) => {
                                    if suffix
                                        .filter(|suffix| !file_name.ends_with(suffix))
                                        .is_none()
                                    {
                                        Some(Self::open(path, config.clone()))
                                    } else {
                                        None
                                    }
                                }
                                None => Some(Err(Error::InvalidPath(path))),
                            }
                        } else {
                            None
                        }
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        schemas.sort_by_key(|schema| (schema.name.clone(), schema.absolute_path.clone()));

        Ok(schemas)
    }

    /// For use with `cargo:rerun-if-changed`
    pub fn absolute_path_str(&self) -> Result<&str, Error> {
        self.absolute_path
            .as_os_str()
            .to_str()
            .ok_or_else(|| Error::InvalidPath(self.absolute_path.clone()))
    }
}

pub fn parse_schema(
    schema_source: &str,
    config: Config,
) -> Result<(GenSchema, SchemaDescriptor), Error> {
    let schema_type = Arc::new(parse_message_type(schema_source)?);
    let descriptor = SchemaDescriptor::new(schema_type);
    let schema = GenSchema::from_schema(&descriptor, config)?;

    Ok((schema, descriptor))
}

const STATIC_SCHEMA_DEF: &str = "lazy_static::lazy_static! {
    pub static ref SCHEMA: parquet::schema::types::SchemaDescPtr =
        std::sync::Arc::new(
            parquet::schema::types::SchemaDescriptor::new(
                std::sync::Arc::new(
                    parquet::schema::parser::parse_message_type(SCHEMA_SOURCE).unwrap()
                )
            )
        );
}";

fn schema_to_scope(
    schema_source: &str,
    schema: &GenSchema,
    descriptor: &SchemaDescriptor,
) -> Result<Scope, Error> {
    let mut scope = Scope::new();

    scope.raw(&format!(
        "const SCHEMA_SOURCE: &str = \"{}\";",
        schema_source
    ));
    scope.raw(STATIC_SCHEMA_DEF);

    for GenStruct {
        type_name,
        fields,
        derives,
    } in schema.structs()
    {
        let gen_struct = scope.new_struct(&type_name).vis("pub");
        for value in &derives {
            gen_struct.derive(value);
        }

        for gen_field in fields {
            let field = gen_struct
                .new_field(&gen_field.name, &gen_field.type_name())
                .vis("pub");

            if let Some(attributes) = gen_field.attributes {
                field.annotation(attributes);
            }
        }
    }

    for gen_column in schema.gen_columns() {
        println!("{:?}", gen_column);
    }

    column_code::add_column_info_modules(&mut scope, &schema.gen_columns());

    let schema_impl = scope
        .new_impl(&schema.type_name)
        .impl_trait("parquetry::Schema")
        .associate_type("SortColumn", "columns::SortColumn");

    schema_impl
        .new_fn("sort_key_value")
        .arg_ref_self()
        .arg("sort_key", "parquetry::SortKey<Self::SortColumn>")
        .ret("Vec<u8>")
        .push_block(code::gen_sort_key_value_block());

    schema_impl
        .new_fn("source")
        .ret("&'static str")
        .line("SCHEMA_SOURCE");

    schema_impl
        .new_fn("schema")
        .ret("parquet::schema::types::SchemaDescPtr")
        .line("SCHEMA.clone()");

    schema_impl
        .new_fn("write")
        .generic("W: std::io::Write + Send")
        .generic("I: IntoIterator<Item = Vec<Self>>")
        .arg("writer", "W")
        .arg("properties", "parquet::file::properties::WriterProperties")
        .arg("groups", "I")
        .ret("Result<parquet::format::FileMetaData, parquetry::error::Error>")
        .push_block(code::gen_write_block()?);

    schema_impl
        .new_fn("write_group")
        .generic("W: std::io::Write + Send")
        .arg(
            "file_writer",
            "&mut parquet::file::writer::SerializedFileWriter<W>",
        )
        .arg("group", "&[Self]")
        .ret("Result<parquet::file::metadata::RowGroupMetaDataPtr, parquetry::error::Error>")
        .push_block(code::gen_write_group_block()?);

    let row_conversion_impl = scope
        .new_impl(&schema.type_name)
        .impl_trait("TryFrom<parquet::record::Row>")
        .associate_type("Error", "parquetry::error::Error");

    row_conversion_impl
        .new_fn("try_from")
        .arg("row", "parquet::record::Row")
        .ret("Result<Self, parquetry::error::Error>")
        .push_block(code::gen_row_conversion_block(schema)?);

    let base_impl = scope.new_impl(&schema.type_name);

    base_impl
        .new_fn("write_sort_key_bytes")
        .arg_ref_self()
        .arg(
            "column",
            "parquetry::Sort<<Self as parquetry::Schema>::SortColumn>",
        )
        .arg("bytes", "&mut Vec<u8>")
        .push_block(code::gen_write_sort_key_bytes_block(schema)?);

    base_impl
        .new_fn("write_with_workspace")
        .generic("W: std::io::Write + Send")
        .arg(
            "file_writer",
            "&mut parquet::file::writer::SerializedFileWriter<W>",
        )
        .arg("workspace", format!("&mut {}", code::WORKSPACE_STRUCT_NAME))
        .ret("Result<parquet::file::metadata::RowGroupMetaDataPtr, parquetry::error::Error>")
        .push_block(code::gen_write_with_workspace_block(descriptor.columns())?);

    base_impl
        .new_fn("fill_workspace")
        .arg("workspace", format!("&mut {}", code::WORKSPACE_STRUCT_NAME))
        .arg("group", "&[Self]")
        .ret("Result<usize, parquetry::error::Error>")
        .push_block(code::gen_fill_workspace_block(schema)?);

    code::add_workspace_struct(&mut scope, descriptor.columns())?;

    if schema.config.tests {
        let test_module = scope.new_module("test").attr("cfg(test)");

        test_code::gen_test_code(test_module, schema)?;
    }

    Ok(scope)
}
