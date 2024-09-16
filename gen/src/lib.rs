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

const STATIC_SCHEMA_DEF: &str = "
    pub static SCHEMA: once_cell::sync::Lazy<parquet::schema::types::SchemaDescPtr> =
        once_cell::sync::Lazy::new(|| std::sync::Arc::new(
            parquet::schema::types::SchemaDescriptor::new(
                std::sync::Arc::new(
                    parquet::schema::parser::parse_message_type(SCHEMA_SOURCE).unwrap()
                )
            )
        ));
";

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

    column_code::add_column_info_modules(&mut scope, &schema.gen_columns());

    let schema_impl = scope
        .new_impl(&schema.type_name)
        .impl_trait("parquetry::Schema")
        .associate_type("SortColumn", "columns::SortColumn")
        .associate_type(
            "Writer<W: std::io::Write + Send>",
            format!("{}Writer<W>", schema.type_name),
        );

    schema_impl
        .new_fn("sort_key_value")
        .arg_ref_self()
        .arg("sort_key", "parquetry::sort::SortKey<Self::SortColumn>")
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
        .new_fn("writer")
        .generic("W: std::io::Write + Send")
        .arg("writer", "W")
        .arg("properties", "parquet::file::properties::WriterProperties")
        .ret("Result<Self::Writer<W>, parquetry::error::Error>")
        .push_block(code::gen_writer_block()?);

    let writer_struct = scope
        .new_struct(&format!("{}Writer", schema.type_name))
        .vis("pub")
        .generic("W: std::io::Write");

    writer_struct.new_field("writer", "parquet::file::writer::SerializedFileWriter<W>");
    writer_struct.new_field("workspace", code::WORKSPACE_STRUCT_NAME);

    let writer_impl = scope
        .new_impl(&format!("{}Writer<W>", schema.type_name))
        .impl_trait(format!("parquetry::SchemaWrite<{}, W>", schema.type_name))
        .generic("W: std::io::Write + Send");

    writer_impl
        .new_fn("write_row_group")
        .generic("'a")
        .generic(format!(
            "E: From<parquetry::error::Error>, I: Iterator<Item = Result<&'a {}, E>>",
            schema.type_name
        ))
        .arg_mut_self()
        .arg("values", "&mut I")
        .ret("Result<parquet::file::metadata::RowGroupMetaDataPtr, E>")
        .bound(&schema.type_name, "'a")
        .push_block(code::gen_writer_write_row_group_block(schema)?);

    writer_impl
        .new_fn("write_item")
        .arg_mut_self()
        .arg("value", format!("&{}", schema.type_name))
        .ret("Result<(), parquetry::error::Error>")
        .line(format!(
            "{}::add_item_to_workspace(&mut self.workspace, value)",
            schema.type_name
        ));

    writer_impl
        .new_fn("finish_row_group")
        .arg_mut_self()
        .ret("Result<parquet::file::metadata::RowGroupMetaDataPtr, parquetry::error::Error>")
        .line(format!(
            "{}::write_with_workspace(&mut self.writer, &mut self.workspace)",
            schema.type_name
        ));

    writer_impl
        .new_fn("finish")
        .arg_self()
        .ret("Result<parquet::format::FileMetaData, parquetry::error::Error>")
        .line("Ok(self.writer.close()?)");

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
            "parquetry::sort::Sort<<Self as parquetry::Schema>::SortColumn>",
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
        .generic("'a")
        .generic("E: From<parquetry::error::Error>, I: Iterator<Item = Result<&'a Self, E>>")
        .arg("workspace", format!("&mut {}", code::WORKSPACE_STRUCT_NAME))
        .arg("values", "I")
        .ret("Result<usize, E>")
        .push_block(code::gen_fill_workspace_block(schema)?);

    base_impl
        .new_fn("add_item_to_workspace")
        .arg("workspace", format!("&mut {}", code::WORKSPACE_STRUCT_NAME))
        .arg("value", "&Self")
        .ret("Result<(), parquetry::error::Error>")
        .push_block(code::gen_add_item_to_workspace_block(schema)?);

    for gen_struct in schema.structs() {
        let base_impl = scope.new_impl(&gen_struct.type_name);

        code::gen_constructor(&gen_struct, base_impl.new_fn("new"))?;
    }

    code::add_workspace_struct(&mut scope, descriptor.columns())?;

    if schema.config.tests {
        let test_module = scope.new_module("test").attr("cfg(test)");

        test_code::gen_test_code(test_module, schema)?;
    }

    Ok(scope)
}
