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
    use std::cmp::Ordering;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct NullLastOption<A>(Option<A>);

    impl<A: Ord> Ord for NullLastOption<A> {
        fn cmp(&self, other: &Self) -> Ordering {
            match (&self.0, &other.0) {
                (None, None) => Ordering::Equal,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(a), Some(b)) => a.cmp(b),
            }
        }
    }

    impl<A: Ord> PartialOrd for NullLastOption<A> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    quickcheck::quickcheck! {
        fn sort_by_key_simple(values: Vec<Simple>) -> bool {
            let mut by_key_bytes = values.clone();
            let mut by_fields = values.clone();

            let sort_key = Simple::sort_key(&[Sort::new(columns::SortColumn::Mno), Sort::new(columns::SortColumn::Abc)]).unwrap();

            by_key_bytes.sort_by_key(|value| value.sort_key_value(sort_key));
            by_fields.sort_by_key(|value| (value.mno, value.abc));

            by_key_bytes == by_fields
        }
    }

    quickcheck::quickcheck! {
        fn sort_by_key_optional(values: Vec<Simple>) -> bool {
            let mut by_key_bytes = values.clone();
            let mut by_fields = values.clone();

            let sort_key = Simple::sort_key(&[Sort::new(columns::SortColumn::Abc), Sort::new(columns::SortColumn::Def)]).unwrap();
            by_key_bytes.sort_by_key(|value| value.sort_key_value(sort_key));
            by_fields.sort_by_key(|value| (value.abc, NullLastOption(value.def.clone())));

            by_key_bytes == by_fields
        }
    }

    quickcheck::quickcheck! {
        fn sort_by_key_optional_nulls_first(values: Vec<Simple>) -> bool {
            let mut by_key_bytes = values.clone();
            let mut by_fields = values.clone();

            let sort_key = Simple::sort_key(&[Sort::new(columns::SortColumn::Def).nulls_first(), Sort::new(columns::SortColumn::Abc)]).unwrap();
            by_key_bytes.sort_by_key(|value| value.sort_key_value(sort_key));
            by_fields.sort_by_key(|value| (value.def.clone(), value.abc));

            by_key_bytes == by_fields
        }
    }
}
