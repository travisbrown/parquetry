use convert_case::{Case, Casing};
use parquet::{
    basic::{LogicalType, Repetition},
    schema::types::{ColumnDescPtr, SchemaDescriptor, Type},
};
use std::collections::HashSet;
use std::ops::Range;

use crate::types::TypeMapping;

use super::{Config, error::Error};

#[derive(Clone, Debug)]
pub struct GenSchema {
    pub type_name: String,
    pub gen_fields: Vec<GenField>,
    pub config: Config,
}

#[derive(Clone, Debug)]
pub struct GenField {
    pub name: String,
    pub base_type_name: String,
    pub attributes: Option<String>,
    pub optional: bool,
    pub gen_type: GenType,
}

#[derive(Clone, Debug)]
pub enum GenType {
    Column(GenColumn),
    Struct {
        gen_fields: Vec<GenField>,
        def_depth: usize,
        rep_depth: usize,
    },
    List {
        element_optional: bool,
        element_gen_type: Box<GenType>,
        element_struct_name: String,
        def_depth: usize,
        rep_depth: usize,
    },
}

#[derive(Clone, Debug)]
pub struct GenStruct {
    pub type_name: String,
    pub fields: Vec<GenField>,
    pub derives: Vec<&'static str>,
}

#[derive(Clone, Debug)]
pub struct GenColumn {
    pub index: usize,
    pub rust_path: Vec<(String, bool)>,
    pub descriptor: ColumnDescPtr,
    pub mapping: TypeMapping,
}

impl GenStruct {
    fn new(
        type_name: &str,
        fields: Vec<GenField>,
        base_derives: &[&'static str],
        disallowed_derives: HashSet<&str>,
    ) -> Self {
        let derives = base_derives
            .iter()
            .cloned()
            .filter(|value| !disallowed_derives.contains(value))
            .collect::<Vec<_>>();

        Self {
            type_name: type_name.to_string(),
            fields,
            derives,
        }
    }
}

impl GenSchema {
    pub fn from_schema(schema: &SchemaDescriptor, config: Config) -> Result<Self, Error> {
        if let GenField {
            base_type_name,
            gen_type: GenType::Struct { gen_fields, .. },
            ..
        } = GenField::from_type(
            &config,
            schema.root_schema(),
            schema.columns(),
            0,
            "",
            vec![],
            0,
            0,
        )?
        .0
        {
            Ok(Self {
                type_name: base_type_name,
                gen_fields,
                config,
            })
        } else {
            Err(Error::InvalidRootSchema(schema.root_schema().clone()))
        }
    }

    pub fn field_names(&self) -> Vec<&str> {
        self.gen_fields
            .iter()
            .map(|gen_field| gen_field.name.as_str())
            .collect()
    }

    pub fn structs(&self) -> Vec<GenStruct> {
        let disallowed_derives = self
            .gen_fields
            .iter()
            .flat_map(|gen_field| gen_field.gen_type.disallowed_derives())
            .collect();

        let mut structs = vec![GenStruct::new(
            &self.type_name,
            self.gen_fields.clone(),
            &self.config.derives(),
            disallowed_derives,
        )];

        for gen_field in &self.gen_fields {
            gen_field.gen_type.structs(
                &gen_field.base_type_name,
                &self.config.derives(),
                &mut structs,
            );
        }

        structs
    }

    pub fn gen_columns(&self) -> Vec<GenColumn> {
        let mut gen_columns = vec![];

        for gen_field in &self.gen_fields {
            gen_field.gen_type.gen_columns(&mut gen_columns);
        }

        gen_columns
    }
}

impl GenField {
    pub fn type_name(&self) -> String {
        if self.optional {
            format!("Option<{}>", self.base_type_name)
        } else {
            self.base_type_name.to_string()
        }
    }

    fn field_name(source_name: &str) -> String {
        source_name.to_string()
    }

    fn field_type_name(source_name: &str) -> String {
        source_name.to_case(Case::Pascal)
    }

    fn from_type(
        config: &Config,
        tp: &Type,
        columns: &[ColumnDescPtr],
        current_column_index: usize,
        name: &str,
        rust_path: Vec<(String, bool)>,
        def_depth: usize,
        rep_depth: usize,
    ) -> Result<(Self, usize), Error> {
        match tp {
            Type::PrimitiveType {
                basic_info,
                physical_type,
                type_length,
                ..
            } => {
                // We currently only support annotated lists
                if basic_info.repetition() == Repetition::REPEATED {
                    Err(Error::UnsupportedRepetition(basic_info.name().to_string()))
                } else {
                    let column = columns[current_column_index].clone();
                    let mapping = super::types::TypeMapping::from_types(
                        column.logical_type(),
                        *physical_type,
                        *type_length,
                    )?;
                    let optional = basic_info.repetition() == Repetition::OPTIONAL;

                    Ok((
                        Self {
                            name: name.to_string(),
                            base_type_name: mapping.rust_type_name().to_string(),
                            attributes: mapping.attributes(config.serde_support, optional),
                            optional,
                            gen_type: GenType::Column(GenColumn {
                                index: current_column_index,
                                rust_path,
                                descriptor: column,
                                mapping,
                            }),
                        },
                        current_column_index + 1,
                    ))
                }
            }
            Type::GroupType { basic_info, fields } => {
                let name = Self::field_name(basic_info.name());
                let optional =
                    basic_info.has_repetition() && basic_info.repetition() == Repetition::OPTIONAL;
                let new_def_depth = def_depth + if optional { 1 } else { 0 };

                if let Some(element_type) =
                    super::util::supported_logical_list_element_type(basic_info, fields)
                {
                    let (element_gen_field, new_current_column_index) = Self::from_type(
                        config,
                        &element_type,
                        columns,
                        current_column_index,
                        &Self::field_name(element_type.get_basic_info().name()),
                        rust_path,
                        new_def_depth + 1,
                        rep_depth + 1,
                    )?;

                    let element_struct_name =
                        Self::field_type_name(&format!("{}_element", basic_info.name()));

                    let element_type_name = match element_gen_field.gen_type {
                        GenType::Column { .. } => element_gen_field.type_name(),
                        GenType::Struct { .. } => {
                            if element_gen_field.optional {
                                format!("Option<{element_struct_name}>")
                            } else {
                                element_struct_name.clone()
                            }
                        }
                        GenType::List { .. } => element_gen_field.type_name(),
                    };

                    Ok((
                        Self {
                            name,
                            base_type_name: format!("Vec<{element_type_name}>"),
                            attributes: None,
                            optional,
                            gen_type: GenType::List {
                                def_depth: new_def_depth + 1,
                                rep_depth: rep_depth + 1,
                                element_optional: element_gen_field.optional,
                                element_gen_type: Box::new(element_gen_field.gen_type),
                                element_struct_name,
                            },
                        },
                        new_current_column_index,
                    ))
                } else if basic_info.logical_type() == Some(LogicalType::List)
                    || (basic_info.has_repetition()
                        && basic_info.repetition() == Repetition::REPEATED)
                {
                    Err(Error::UnsupportedRepetition(basic_info.name().to_string()))
                } else {
                    let mut gen_fields = vec![];
                    let mut new_current_column_index = current_column_index;

                    for field in fields {
                        let name = Self::field_name(field.get_basic_info().name());
                        let mut rust_path = rust_path.clone();
                        rust_path.push((name.clone(), field.is_optional()));
                        let (gen_field, column_index) = Self::from_type(
                            config,
                            field,
                            columns,
                            new_current_column_index,
                            &name,
                            rust_path,
                            new_def_depth,
                            rep_depth,
                        )?;
                        new_current_column_index = column_index;
                        gen_fields.push(gen_field);
                    }

                    Ok((
                        Self {
                            name,
                            base_type_name: Self::field_type_name(basic_info.name()),
                            attributes: None,
                            optional,
                            gen_type: GenType::Struct {
                                gen_fields,
                                def_depth: new_def_depth,
                                rep_depth,
                            },
                        },
                        new_current_column_index,
                    ))
                }
            }
        }
    }
}

impl GenType {
    pub fn column_indices(&self) -> Range<usize> {
        match self {
            GenType::Column(GenColumn { index, .. }) => *index..*index + 1,
            GenType::Struct { gen_fields, .. } => {
                let mut start = usize::MAX;
                let mut end = usize::MIN;

                for gen_field in gen_fields {
                    let range = gen_field.gen_type.column_indices();
                    start = start.min(range.start);
                    end = end.max(range.end);
                }
                start..end
            }
            GenType::List {
                element_gen_type, ..
            } => element_gen_type.column_indices(),
        }
    }

    pub fn repeated_column_indices(&self) -> Vec<usize> {
        match self {
            GenType::Column(GenColumn {
                index, descriptor, ..
            }) => {
                if descriptor.max_rep_level() > 0 {
                    vec![*index]
                } else {
                    vec![]
                }
            }
            GenType::Struct { gen_fields, .. } => {
                let mut indices = vec![];

                for gen_field in gen_fields {
                    indices.extend(gen_field.gen_type.repeated_column_indices());
                }

                indices.sort();
                indices.dedup();
                indices
            }
            GenType::List {
                element_gen_type, ..
            } => element_gen_type.repeated_column_indices(),
        }
    }

    fn disallowed_derives(&self) -> HashSet<&'static str> {
        let mut values = HashSet::new();

        match self {
            GenType::Column(GenColumn { mapping, .. }) => {
                values.extend(&mapping.disallowed_derives());
            }
            GenType::Struct { gen_fields, .. } => {
                for gen_field in gen_fields {
                    values.extend(gen_field.gen_type.disallowed_derives());
                }
            }
            GenType::List {
                element_gen_type, ..
            } => {
                values.insert("Copy");
                values.extend(element_gen_type.disallowed_derives());
            }
        }

        values
    }

    fn structs(&self, type_name: &str, base_derives: &[&'static str], acc: &mut Vec<GenStruct>) {
        match self {
            GenType::Column { .. } => {}
            GenType::Struct { gen_fields, .. } => {
                acc.push(GenStruct::new(
                    type_name,
                    gen_fields.clone(),
                    base_derives,
                    self.disallowed_derives(),
                ));

                for GenField {
                    base_type_name,
                    gen_type,
                    ..
                } in gen_fields
                {
                    gen_type.structs(base_type_name, base_derives, acc);
                }
            }
            GenType::List {
                element_gen_type,
                element_struct_name,
                ..
            } => element_gen_type.structs(element_struct_name, base_derives, acc),
        }
    }

    fn gen_columns(&self, acc: &mut Vec<GenColumn>) {
        match self {
            GenType::Column(column) => {
                acc.push(column.clone());
            }
            GenType::Struct { gen_fields, .. } => {
                for gen_field in gen_fields {
                    gen_field.gen_type.gen_columns(acc);
                }
            }
            GenType::List {
                element_gen_type, ..
            } => {
                element_gen_type.gen_columns(acc);
            }
        }
    }
}

impl GenColumn {
    pub fn variant_name(&self) -> String {
        self.rust_path.last().unwrap().0.to_case(Case::Pascal)
    }

    pub fn is_sort_column(&self) -> bool {
        self.descriptor.max_rep_level() == 0
    }
}
