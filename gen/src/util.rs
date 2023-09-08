use parquet::{
    basic::{LogicalType, Repetition},
    schema::types::{BasicTypeInfo, TypePtr},
};

/// If this type is a supported list type, return the element type
pub fn supported_logical_list_element_type(
    type_info: &BasicTypeInfo,
    fields: &[TypePtr],
) -> Option<TypePtr> {
    if type_info.logical_type() == Some(LogicalType::List)
        && fields.len() == 1
        && fields[0].is_group()
        && fields[0].name() == "list"
        && fields[0].get_basic_info().has_repetition()
        && fields[0].get_basic_info().repetition() == Repetition::REPEATED
        && fields[0].get_fields().len() == 1
        && fields[0].get_fields()[0].name() == "element"
    {
        Some(fields[0].get_fields()[0].clone())
    } else {
        None
    }
}
