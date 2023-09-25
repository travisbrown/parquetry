#[allow(dead_code)]
mod nested;
#[allow(dead_code)]
mod simple;
#[allow(dead_code)]
mod two_list_levels;

#[cfg(test)]
mod test {
    use super::simple::{columns, Simple};
    use parquetry::{Schema, Sort};

    quickcheck::quickcheck! {
        fn sort_by_key_01(values: Vec<Simple>) -> bool {
            let mut by_key_bytes = values.clone();
            let mut by_fields = values.clone();

            let sort_key = Simple::sort_key(&[Sort::new(columns::SortColumn::Mno), Sort::new(columns::SortColumn::Abc)]).unwrap();

            by_key_bytes.sort_by_key(|value| value.sort_key_value(sort_key));
            by_fields.sort_by_key(|value| (value.mno, value.abc));

            by_key_bytes == by_fields
        }
    }
}
