use codegen::{Block, Function, Scope};
use parquet::{basic::Type as PhysicalType, schema::types::ColumnDescPtr};

use super::{
    error::Error,
    schema::{GenColumn, GenField, GenSchema, GenStruct, GenType},
    types::{DateTimeUnit, TypeMapping},
};

pub const WORKSPACE_STRUCT_NAME: &str = "ParquetryWorkspace";

pub fn gen_constructor(gen_struct: &GenStruct, function: &mut Function) -> Result<(), Error> {
    function
        .vis("pub")
        .ret("Result<Self, parquetry::error::ValueError>");

    for field in &gen_struct.fields {
        function.arg(&field.name, field.type_name());

        if let GenType::Column(column) = &field.gen_type {
            match column.mapping {
                TypeMapping::DateTime(date_time_unit) => {
                    let digits = match date_time_unit {
                        DateTimeUnit::Millis => 3,
                        DateTimeUnit::Micros => 6,
                    };

                    if field.optional {
                        function.line(format!(
                            "let {} = {}.map(|value| chrono::SubsecRound::trunc_subsecs(value, {digits}));",
                            field.name, field.name));
                    } else {
                        function.line(format!(
                            "let {} = chrono::SubsecRound::trunc_subsecs({}, {digits});",
                            field.name, field.name
                        ));
                    }
                }
                TypeMapping::String => {
                    let mut column_path_code = String::new();
                    column_path_code.push_str("parquet::schema::types::ColumnPath::new(vec![");
                    for part in column.descriptor.path().parts() {
                        column_path_code.push_str(&format!("\"{part}\".to_string(), "));
                    }
                    column_path_code.push_str("])");

                    if field.optional {
                        function.line(format!(
                            "if let Some({}) = {}.as_ref() {{",
                            field.name, field.name
                        ));
                    }

                    function.line(format!(
                        "for (index, byte) in {}.as_bytes().iter().enumerate() {{",
                        field.name
                    ));
                    function.line("if *byte == 0 {");
                    function.line(format!(
                        "return Err(parquetry::error::ValueError::NullByteString {{ column_path: {column_path_code}, index }});"
                    ));

                    if field.optional {
                        function.line("}");
                    }

                    function.line("}");
                    function.line("}");
                }
                _ => {}
            }
        }
    }

    function.line("Ok(Self {");

    for field in &gen_struct.fields {
        function.line(format!("{}, ", field.name));
    }

    function.line("})");

    Ok(())
}

pub fn gen_field_writer_code(
    gen_field: &GenField,
    rep_level: Option<usize>,
) -> Result<Vec<String>, Error> {
    gen_type_writer_code(
        &gen_field.gen_type,
        &gen_field.name,
        &gen_field.base_type_name,
        gen_field.optional,
        rep_level,
    )
}

fn gen_push<A: AsRef<str>, B: ToString>(name: A, value: B) -> String {
    format!("{}.push({});", name.as_ref(), value.to_string())
}

fn gen_option_match<A: AsRef<str>, B: ToString>(
    name: A,
    struct_info: Option<(A, &[A])>,
    some_value: B,
    none_value: B,
) -> String {
    let binding = match struct_info {
        Some((type_name, field_names)) => format!(
            "{} {{ {} }}",
            type_name.as_ref(),
            field_names
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => name.as_ref().to_string(),
    };

    format!(
        "match {} {{ Some({binding}) => {{ {} }}, None => {{ {} }} }}",
        name.as_ref(),
        some_value.to_string(),
        none_value.to_string()
    )
}

fn gen_type_writer_code(
    gen_type: &GenType,
    name: &str,
    base_type_name: &str,
    optional: bool,
    rep_level: Option<usize>,
) -> Result<Vec<String>, Error> {
    let code = match gen_type {
        GenType::Column(GenColumn {
            index,
            descriptor,
            mapping,
            ..
        }) => {
            let assignment = gen_push(
                format!("workspace.{}", values_var_name(*index)),
                mapping.physical_type_conversion(name),
            );

            let mut code = vec![];

            if !optional {
                code.push(assignment);
                if descriptor.max_def_level() > 0 {
                    code.push(gen_push(
                        format!("workspace.{}", def_levels_var_name(*index)),
                        descriptor.max_def_level(),
                    ))
                };
            } else {
                let some_code = [
                    assignment,
                    gen_push(
                        format!("workspace.{}", def_levels_var_name(*index)),
                        descriptor.max_def_level(),
                    ),
                ];
                let none_code = gen_push(
                    format!("workspace.{}", def_levels_var_name(*index)),
                    descriptor.max_def_level() - 1,
                );

                code.push(gen_option_match(
                    name,
                    None,
                    some_code.join("\n"),
                    none_code,
                ));
            }

            if let Some(rep_level) = rep_level {
                code.push(gen_push(
                    format!("workspace.{}", rep_levels_var_name(*index)),
                    rep_level,
                ));
            }

            code
        }
        GenType::Struct {
            gen_fields,
            def_depth,
            rep_depth,
        } => {
            let field_names = gen_fields
                .iter()
                .map(|gen_field| gen_field.name.as_str())
                .collect::<Vec<_>>();

            if !optional {
                let mut code = vec![format!(
                    "let {base_type_name} {{ {} }} = {name};",
                    field_names.join(", "),
                )];

                for field in gen_fields {
                    code.extend(gen_field_writer_code(field, rep_level)?);
                }

                code
            } else {
                let mut some_code = vec![];
                let mut none_code = vec![];

                for field in gen_fields {
                    some_code.extend(gen_field_writer_code(field, rep_level)?);
                }

                for index in gen_type.column_indices() {
                    none_code.push(gen_push(
                        format!("workspace.{}", def_levels_var_name(index)),
                        def_depth - 1,
                    ));
                }

                for index in gen_type.repeated_column_indices() {
                    none_code.push(gen_push(
                        format!("workspace.{}", rep_levels_var_name(index)),
                        rep_level.unwrap_or(*rep_depth),
                    ));
                }

                let code = gen_option_match(
                    name,
                    Some((base_type_name, &field_names)),
                    some_code.join("\n"),
                    none_code.join("\n"),
                );

                vec![code]
            }
        }
        GenType::List {
            def_depth,
            rep_depth,
            element_optional,
            element_gen_type,
            element_struct_name,
        } => {
            let mut empty_code = vec![];
            for index in gen_type.column_indices() {
                empty_code.push(gen_push(
                    format!("workspace.{}", def_levels_var_name(index)),
                    def_depth - 1,
                ));
            }

            for index in gen_type.repeated_column_indices() {
                empty_code.push(gen_push(
                    format!("workspace.{}", rep_levels_var_name(index)),
                    rep_level.unwrap_or(rep_depth - 1),
                ));
            }

            let mut non_empty_code = vec!["if first {".to_string()];
            non_empty_code.extend(gen_type_writer_code(
                element_gen_type,
                "element",
                element_struct_name,
                *element_optional,
                rep_level.or(Some(*rep_depth - 1)),
            )?);
            non_empty_code.push("first = false;".to_string());
            non_empty_code.push("} else {".to_string());
            non_empty_code.extend(gen_type_writer_code(
                element_gen_type,
                "element",
                element_struct_name,
                *element_optional,
                Some(*rep_depth),
            )?);
            non_empty_code.push("}".to_string());

            let mut code = vec![format!("if {name}.is_empty() {{")];
            code.extend(empty_code);
            code.push("} else {".to_string());
            code.push("let mut first = true;".to_string());
            code.push(format!("for element in {name} {{"));
            code.extend(non_empty_code);
            code.push("}".to_string());
            code.push("}".to_string());

            if !optional {
                code
            } else {
                let mut none_code = vec![];
                for index in gen_type.column_indices() {
                    none_code.push(gen_push(
                        format!("workspace.{}", def_levels_var_name(index)),
                        def_depth - 2,
                    ));
                }

                for index in gen_type.repeated_column_indices() {
                    none_code.push(gen_push(
                        format!("workspace.{}", rep_levels_var_name(index)),
                        rep_level.unwrap_or(rep_depth - 1),
                    ));
                }

                vec![gen_option_match(
                    name,
                    None,
                    code.join("\n"),
                    none_code.join("\n"),
                )]
            }
        }
    };

    Ok(code)
}

pub fn gen_row_conversion_block(gen_schema: &GenSchema) -> Result<Block, Error> {
    let mut block = Block::new("");

    for line in
        gen_row_conversion_assignments(&gen_schema.type_name, &gen_schema.gen_fields, false)?
    {
        block.line(line);
    }

    Ok(block)
}

fn gen_row_match_lines(
    gen_type: &GenType,
    field_name: &str,
    base_type_name: &str,
    optional: bool,
) -> Result<Vec<String>, Error> {
    let mut lines = vec![];

    if optional {
        lines.push("parquet::record::Field::Null => Ok(None),".to_string());
    }

    match gen_type {
        GenType::Column(GenColumn { mapping, .. }) => {
            if optional {
                lines.push(format!(
                    "parquet::record::Field::{}({}) => Ok(Some({})),",
                    mapping.row_field_variant(),
                    "value",
                    mapping.row_field_conversion(field_name, "value")
                ));
            } else {
                lines.push(format!(
                    "parquet::record::Field::{}({}) => Ok({}),",
                    mapping.row_field_variant(),
                    "value",
                    mapping.row_field_conversion(field_name, "value")
                ));
            }
        }
        GenType::Struct { gen_fields, .. } => {
            lines.push("parquet::record::Field::Group(row) => {".to_string());
            lines.extend(gen_row_conversion_assignments(
                base_type_name,
                gen_fields,
                optional,
            )?);
            lines.push("}".to_string());
        }
        GenType::List {
            element_optional,
            element_gen_type,
            element_struct_name,
            ..
        } => {
            lines.push("parquet::record::Field::ListInternal(fields) => {".to_string());
            lines.push("let mut values = Vec::with_capacity(fields.len());".to_string());

            lines.push("for field in fields.elements() {".to_string());
            lines.push("let value = match field {".to_string());

            lines.extend(gen_row_match_lines(
                element_gen_type,
                field_name,
                element_struct_name,
                *element_optional,
            )?);

            lines.push(format!(
                "_ => Err(parquetry::error::Error::InvalidField(\"{field_name}\".to_string()))",
            ));

            lines.push("}?;".to_string());
            lines.push("values.push(value);".to_string());
            lines.push("}".to_string());

            if optional {
                lines.push("Ok(Some(values))".to_string());
            } else {
                lines.push("Ok(values)".to_string());
            }
            lines.push("}".to_string());
        }
    }

    Ok(lines)
}

fn gen_row_conversion_assignments(
    type_name: &str,
    gen_fields: &[GenField],
    optional: bool,
) -> Result<Vec<String>, Error> {
    let mut lines = vec!["let mut fields = row.get_column_iter();".to_string()];

    for gen_field in gen_fields {
        lines.push(format!(
            "let {} = match fields.next().ok_or_else(||",
            gen_field.name
        ));
        lines.push(format!(
            "parquetry::error::Error::InvalidField(\"{}\".to_string()))?.1 {{",
            gen_field.name
        ));

        lines.extend(gen_row_match_lines(
            &gen_field.gen_type,
            &gen_field.name,
            &gen_field.base_type_name,
            gen_field.optional,
        )?);

        lines.push(format!(
            "_ => Err(parquetry::error::Error::InvalidField(\"{}\".to_string()))",
            gen_field.name
        ));
        lines.push("}?;".to_string());
    }

    let value_base = format!(
        "{type_name} {{ {} }}",
        gen_fields
            .iter()
            .map(|gen_field| gen_field.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    if optional {
        lines.push(format!("Ok(Some({value_base}))"));
    } else {
        lines.push(format!("Ok({value_base})"));
    }

    Ok(lines)
}

pub fn gen_writer_block() -> Result<Block, Error> {
    let mut block = Block::new("");

    block.line("Ok(Self::Writer {");
    block.line("writer: parquet::file::writer::SerializedFileWriter::new(writer, SCHEMA.root_schema_ptr(), std::sync::Arc::new(properties))?,");
    block.line("workspace: Default::default()");
    block.line("})");

    Ok(block)
}

pub fn gen_writer_write_row_group_block(gen_schema: &GenSchema) -> Result<Block, Error> {
    let mut block = Block::new("");

    block.line(format!(
        "{}::fill_workspace(&mut self.workspace, values)?;",
        gen_schema.type_name
    ));
    block.line(format!(
        "{}::write_with_workspace(&mut self.writer, &mut self.workspace).map_err(E::from)",
        gen_schema.type_name
    ));

    Ok(block)
}

pub fn gen_fill_workspace_block() -> Result<Block, Error> {
    let mut block = Block::new("");
    block.line("let mut written_count = 0;");
    block.line("for result in values {");
    block.line("Self::add_item_to_workspace(workspace, result?)?;");
    block.line("written_count += 1;");
    block.line("}");

    block.line("Ok(written_count)");

    Ok(block)
}

pub fn gen_add_item_to_workspace_block(gen_schema: &GenSchema) -> Result<Block, Error> {
    let mut block = Block::new("");

    block.line(format!(
        "let {} {{ {} }} = value;",
        gen_schema.type_name,
        gen_schema.field_names().join(", ")
    ));

    for gen_field in &gen_schema.gen_fields {
        for line in gen_field_writer_code(gen_field, None)? {
            block.line(line);
        }
    }

    block.line("Ok(())");

    Ok(block)
}

pub fn gen_write_with_workspace_block(columns: &[ColumnDescPtr]) -> Result<Block, Error> {
    let mut block = Block::new("");
    block.line("let mut row_group_writer = file_writer.next_row_group()?;");

    for (index, column) in columns.iter().enumerate() {
        block.line("let mut column_writer = ");
        block.line(format!("row_group_writer.next_column()?.ok_or_else(|| parquetry::error::Error::InvalidField(\"{}\".to_string()))?;", column.name()));
        block.line(format!(
            "column_writer.typed::<parquet::data_type::{}>().write_batch(",
            physical_type_name(column.physical_type())?
        ));

        block.line(format!("&workspace.{},", values_var_name(index)));

        if column.max_def_level() > 0 {
            block.line(format!("Some(&workspace.{}),", def_levels_var_name(index)));
        } else {
            block.line("None,");
        }

        if column.max_rep_level() > 0 {
            block.line(format!("Some(&workspace.{}),", rep_levels_var_name(index)));
        } else {
            block.line("None,");
        }

        block.line(")?;");
        block.line("column_writer.close()?;");
    }

    block.line("workspace.clear();");
    block.line("Ok(row_group_writer.close()?)");

    Ok(block)
}

pub fn add_workspace_struct(scope: &mut Scope, columns: &[ColumnDescPtr]) -> Result<(), Error> {
    let workspace = scope.new_struct(WORKSPACE_STRUCT_NAME);
    let mut clear_code = vec![];

    workspace.derive("Default");

    for (index, column) in columns.iter().enumerate() {
        let values_name = values_var_name(index);
        workspace.new_field(
            &values_name,
            format!(
                "Vec<{}>",
                physical_type_rust_type_name(column.physical_type())?
            ),
        );
        clear_code.push(format!("self.{values_name}.clear();"));

        if column.max_def_level() > 0 {
            let def_levels_name = def_levels_var_name(index);
            workspace.new_field(&def_levels_name, "Vec<i16>");
            clear_code.push(format!("self.{def_levels_name}.clear();"));
        }

        if column.max_rep_level() > 0 {
            let rep_levels_name = rep_levels_var_name(index);
            workspace.new_field(&rep_levels_name, "Vec<i16>");
            clear_code.push(format!("self.{rep_levels_name}.clear();"));
        }
    }

    let workspace_impl = scope
        .new_impl(WORKSPACE_STRUCT_NAME)
        .new_fn("clear")
        .arg_mut_self();

    for line in clear_code {
        workspace_impl.line(line);
    }

    Ok(())
}

pub fn gen_sort_key_value_block() -> Block {
    let mut block = Block::new("");
    block.line("let mut bytes = vec![];");
    block.line("for column in sort_key.columns() {");
    block.line("self.write_sort_key_bytes(column, &mut bytes);");
    block.line("}");
    block.line("bytes");
    block
}

pub fn gen_write_sort_key_bytes_block(schema: &GenSchema) -> Result<Block, Error> {
    let mut block = Block::new("match column.column");
    for gen_column in schema
        .gen_columns()
        .iter()
        .filter(|gen_column| gen_column.is_sort_column())
    {
        let mut value_path = String::new();
        let mut any_optional = false;
        for (index, (part, optional)) in gen_column.rust_path.iter().enumerate() {
            let last = index == gen_column.rust_path.len() - 1;

            if any_optional {
                if *optional {
                    if last && gen_column.mapping.is_copy() {
                        value_path.push_str(&format!(".and_then(|value| value.{part})"));
                    } else {
                        value_path.push_str(&format!(".and_then(|value| value.{part}.as_ref())"));
                    }
                } else if last && gen_column.mapping.is_copy() {
                    value_path.push_str(&format!(".map(|value| value.{part})"));
                } else {
                    value_path.push_str(&format!(".map(|value| &value.{part})"));
                }
            } else {
                value_path.push_str(&format!(".{part}"));

                if *optional {
                    if !last || !gen_column.mapping.is_copy() {
                        value_path.push_str(".as_ref()");
                    }
                    any_optional = true;
                }
            }
        }

        let mut code = String::new();

        if any_optional {
            code.push_str("match value {");
            code.push_str("Some(value) => {");
            code.push_str("bytes.push(if column.nulls_first { 1 } else { 0 });");
            code.push_str(&gen_column.mapping.write_bytes());
            code.push('}');
            code.push_str("None => { bytes.push(if column.nulls_first { 0 } else { 1 }); }");
            code.push('}');
        } else {
            code.push_str(&gen_column.mapping.write_bytes());
        }
        if !any_optional && gen_column.rust_path.len() == 1 && !gen_column.mapping.is_copy() {
            block.line(format!(
                "columns::SortColumn::{} => {{ let value = &self{value_path}; {code} }},",
                gen_column.variant_name(),
            ));
        } else {
            block.line(format!(
                "columns::SortColumn::{} => {{ let value = self{value_path}; {code} }},",
                gen_column.variant_name(),
            ));
        }
    }
    Ok(block)
}

fn physical_type_name(t: PhysicalType) -> Result<&'static str, Error> {
    match t {
        PhysicalType::BOOLEAN => Ok("BoolType"),
        PhysicalType::INT32 => Ok("Int32Type"),
        PhysicalType::INT64 => Ok("Int64Type"),
        PhysicalType::FLOAT => Ok("FloatType"),
        PhysicalType::DOUBLE => Ok("DoubleType"),
        PhysicalType::BYTE_ARRAY => Ok("ByteArrayType"),
        PhysicalType::FIXED_LEN_BYTE_ARRAY => Ok("FixedLenByteArrayType"),
        PhysicalType::INT96 => Err(Error::UnsupportedPhysicalType(PhysicalType::INT96)),
    }
}

fn physical_type_rust_type_name(t: PhysicalType) -> Result<&'static str, Error> {
    match t {
        PhysicalType::BOOLEAN => Ok("bool"),
        PhysicalType::INT32 => Ok("i32"),
        PhysicalType::INT64 => Ok("i64"),
        PhysicalType::FLOAT => Ok("f32"),
        PhysicalType::DOUBLE => Ok("f64"),
        PhysicalType::BYTE_ARRAY => Ok("parquet::data_type::ByteArray"),
        PhysicalType::FIXED_LEN_BYTE_ARRAY => Ok("parquet::data_type::FixedLenByteArray"),
        PhysicalType::INT96 => Err(Error::UnsupportedPhysicalType(PhysicalType::INT96)),
    }
}

pub fn values_var_name(index: usize) -> String {
    format!("values_{index:04}")
}

pub fn def_levels_var_name(index: usize) -> String {
    format!("def_levels_{index:04}")
}

pub fn rep_levels_var_name(index: usize) -> String {
    format!("rep_levels_{index:04}")
}
