#[allow(dead_code)]
mod nested;
#[allow(dead_code)]
mod simple;
#[allow(dead_code)]
mod two_list_levels;

#[cfg(test)]
mod test {
    use super::simple::{Simple, columns};
    use chrono::{DateTime, Utc};
    use parquet::file::properties::WriterProperties;
    use parquetry::{Schema, sort::Sort};
    use std::cmp::{Ordering, Reverse};

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

    quickcheck::quickcheck! {
        fn sort_by_key_optional_nulls_first_desc(values: Vec<Simple>) -> bool {
            let mut by_key_bytes = values.clone();
            let mut by_fields = values.clone();

            let sort_key = Simple::sort_key(&[Sort::new(columns::SortColumn::Def).nulls_first().descending(), Sort::new(columns::SortColumn::Abc).descending()]).unwrap();
            by_key_bytes.sort_by_key(|value| value.sort_key_value(sort_key));
            by_fields.sort_by_key(|value| (value.def.clone().map(Reverse), Reverse(value.abc)));

            by_key_bytes == by_fields
        }
    }

    quickcheck::quickcheck! {
        fn sort_db_by_key_simple(values: Vec<Simple>) -> bool {
            let test_db_dir = tempfile::Builder::new().prefix("Simple-sort-db").tempdir().unwrap();
            let test_parquet_dir = tempfile::Builder::new().prefix("Simple-sort-data").tempdir().unwrap();
            let test_file_path = test_parquet_dir.path().join("sort-data.parquet");

            let sort_key = Simple::sort_key(&[Sort::new(columns::SortColumn::Mno), Sort::new(columns::SortColumn::Abc)]).unwrap();
            let sort_db = parquetry_sort::SortDb::open(test_db_dir.path(), sort_key).unwrap();

            for value in &values {
                sort_db.insert(value).unwrap();
            }

            sort_db.write_file(&test_file_path, WriterProperties::builder(), 1028 * 1028, |_| 1, false).unwrap();

            let read_file = std::fs::File::open(test_file_path).unwrap();
            let read_options = parquet::file::serialized_reader::ReadOptionsBuilder::new().build();
            let read_values = Simple::read(read_file, read_options).collect::<Result<Vec<_>, _>>().unwrap();

            let mut by_fields = values.clone();
            by_fields.sort_by_key(|value| (value.mno, value.abc));

            read_values == by_fields
        }
    }

    #[test]
    fn sort_db_colliding_key_stability() -> Result<(), Box<dyn std::error::Error>> {
        let test_db_dir = tempfile::Builder::new()
            .prefix("Simple-stable-sort-db")
            .tempdir()?;
        let test_parquet_dir = tempfile::Builder::new()
            .prefix("Simple-stable-sort-data")
            .tempdir()?;
        let test_file_path = test_parquet_dir.path().join("stable-sort-data.parquet");

        let sort_key = Simple::sort_key(&[
            Sort::new(columns::SortColumn::Abc),
            Sort::new(columns::SortColumn::Def),
        ])?;

        let now = Utc::now();

        let first = vec![
            simple_instance(123, "foo", now)?,
            simple_instance(123, "abc", now)?,
        ];

        // To guard against the bug, this has to be large enough to ensure that all the merges are not done at once.
        let garbage = (0..123)
            .chain(124..10000)
            .map(|abc| simple_instance(abc, "other", now))
            .collect::<Result<Vec<_>, _>>()?;

        let last = vec![
            simple_instance(123, "", now)?,
            simple_instance(123, "foox", now)?,
        ];

        let values = first
            .into_iter()
            .chain(garbage.into_iter())
            .chain(last.into_iter())
            .collect::<Vec<_>>();

        let mut expected = values.clone();

        expected.sort_by_key(|simple| simple.abc);

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(512);

        let sort_db =
            parquetry_sort::SortDb::<Simple>::open_opt(test_db_dir.path(), sort_key, options)?;

        for value in &values {
            sort_db.insert(value)?;
        }

        sort_db.write_file(
            &test_file_path,
            WriterProperties::builder(),
            1028 * 1028,
            |_| 1,
            false,
        )?;

        let read_file = std::fs::File::open(test_file_path)?;
        let read_options = parquet::file::serialized_reader::ReadOptionsBuilder::new().build();
        let read_values = Simple::read(read_file, read_options).collect::<Result<Vec<_>, _>>()?;

        println!("{:?}", read_values);

        assert_eq!(read_values, expected);

        Ok(())
    }

    fn simple_instance(
        abc: u64,
        req_def: &str,
        stu: DateTime<Utc>,
    ) -> Result<Simple, parquetry::error::ValueError> {
        Simple::new(
            abc,
            None,
            req_def.to_string(),
            vec![None],
            None,
            false,
            None,
            stu,
            None,
            0.0,
            [0; 20],
            None,
            None,
        )
    }
}
