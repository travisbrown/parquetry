#[allow(dead_code)]
mod nested;
#[allow(dead_code)]
mod simple;
#[allow(dead_code)]
mod two_list_levels;

#[cfg(test)]
mod tests {
    use chrono::{SubsecRound, Utc};
    use parquet::file::serialized_reader::ReadOptionsBuilder;
    use parquetry::Schema;
    use quickcheck::{quickcheck, Arbitrary, Gen};
    use std::fs::File;

    use super::{nested, simple, two_list_levels};

    #[test]
    fn two_list_levels_round_trip_single() {
        let groups: Vec<Vec<two_list_levels::TwoListLevels>> =
            vec![vec![two_list_levels::TwoListLevels {
                values: vec![vec![]],
            }]];

        let test_dir = tempdir::TempDir::new("two-list-single-levels-test-data").unwrap();
        let test_file_path = test_dir.path().join("two_list_levels.parquet");
        let test_file = File::create(&test_file_path).unwrap();

        two_list_levels::TwoListLevels::write(test_file, Default::default(), groups.clone())
            .unwrap();

        let read_file = File::open(test_file_path).unwrap();
        let read_options = ReadOptionsBuilder::new().build();
        let read_values = two_list_levels::TwoListLevels::read(read_file, read_options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            read_values,
            groups.into_iter().flatten().collect::<Vec<_>>()
        );
    }

    #[test]
    fn nested_round_trip_single() {
        let groups: Vec<Vec<nested::Nested>> = vec![vec![nested::Nested {
            a: 0,
            bc: Utc::now().trunc_subsecs(3),
            cde: None,
            foox: None,
        }]];

        let test_dir = tempdir::TempDir::new("nested-single-test-data").unwrap();
        let test_file_path = test_dir.path().join("nested.parquet");
        let test_file = File::create(&test_file_path).unwrap();

        nested::Nested::write(test_file, Default::default(), groups.clone()).unwrap();

        let read_file = File::open(test_file_path).unwrap();
        let read_options = ReadOptionsBuilder::new().build();
        let read_values = nested::Nested::read(read_file, read_options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            read_values,
            groups.into_iter().flatten().collect::<Vec<_>>()
        );
    }

    quickcheck! {
        fn simple_round_trip(groups: Vec<Vec<SimpleWrapper>>) -> bool {
            let groups: Vec<Vec<simple::Simple>> =
                groups.into_iter().map(|values| values.into_iter().map(|value| value.0).collect()).collect();

            let test_dir = tempdir::TempDir::new("simple-test-data").unwrap();
            let test_file_path = test_dir.path().join("simple.parquet");
            let test_file = File::create(&test_file_path).unwrap();

            simple::Simple::write(test_file, Default::default(), groups.clone()).unwrap();

            let read_file = File::open(test_file_path).unwrap();
            let read_options = ReadOptionsBuilder::new().build();
            let read_values = simple::Simple::read(read_file, read_options).collect::<Result<Vec<_>, _>>().unwrap();

            read_values == groups.into_iter().flatten().collect::<Vec<_>>()
        }
    }

    quickcheck! {
        fn two_list_levels_round_trip(groups: Vec<Vec<TwoListLevelsWrapper>>) -> bool {
            let groups: Vec<Vec<two_list_levels::TwoListLevels>> =
                groups.into_iter().map(|values| values.into_iter().map(|value| value.0).collect()).collect();

            let test_dir = tempdir::TempDir::new("two-list-levels-test-data").unwrap();
            let test_file_path = test_dir.path().join("two_list_levels.parquet");
            let test_file = File::create(&test_file_path).unwrap();

            two_list_levels::TwoListLevels::write(test_file, Default::default(), groups.clone()).unwrap();

            let read_file = File::open(test_file_path).unwrap();
            let read_options = ReadOptionsBuilder::new().build();
            let read_values = two_list_levels::TwoListLevels::read(read_file, read_options).collect::<Result<Vec<_>, _>>().unwrap();

            read_values == groups.into_iter().flatten().collect::<Vec<_>>()
        }
    }

    quickcheck! {
        fn nested_round_trip(groups: Vec<Vec<NestedWrapper>>) -> bool {
            let groups: Vec<Vec<nested::Nested>> =
                groups.into_iter().map(|values| values.into_iter().map(|value| value.0).collect()).collect();

            let test_dir = tempdir::TempDir::new("nested-test-data").unwrap();
            let test_file_path = test_dir.path().join("nested.parquet");
            let test_file = File::create(&test_file_path).unwrap();

            nested::Nested::write(test_file, Default::default(), groups.clone()).unwrap();

            let read_file = File::open(test_file_path).unwrap();
            let read_options = ReadOptionsBuilder::new().build();
            let read_values = nested::Nested::read(read_file, read_options).collect::<Result<Vec<_>, _>>().unwrap();

            read_values == groups.into_iter().flatten().collect::<Vec<_>>()
        }
    }

    quickcheck! {
        fn nested_write_group_round_trip(groups: Vec<Vec<NestedWrapper>>) -> bool {
            let groups: Vec<Vec<nested::Nested>> =
                groups.into_iter().map(|values| values.into_iter().map(|value| value.0).collect()).collect();

            let test_dir = tempdir::TempDir::new("nested-group-write-test-data").unwrap();
            let test_file_path = test_dir.path().join("nested.parquet");
            let test_file = File::create(&test_file_path).unwrap();

            let mut file_writer = parquet::file::writer::SerializedFileWriter::new(
                test_file,
                nested::Nested::schema(),
                Default::default(),
            ).unwrap();

            for group in &groups {
                nested::Nested::write_group(&mut file_writer, group).unwrap();
            }
            file_writer.close().unwrap();

            let read_file = File::open(test_file_path).unwrap();
            let read_options = ReadOptionsBuilder::new().build();
            let read_values = nested::Nested::read(read_file, read_options).collect::<Result<Vec<_>, _>>().unwrap();

            read_values == groups.into_iter().flatten().collect::<Vec<_>>()
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct SimpleWrapper(simple::Simple);

    impl Arbitrary for SimpleWrapper {
        fn arbitrary(g: &mut Gen) -> Self {
            let vwx_arb: Option<f64> = Option::arbitrary(g);
            let yza_arb = f32::arbitrary(g);
            Self(simple::Simple {
                abc: u64::arbitrary(g),
                def: Option::arbitrary(g),
                ghi: Vec::arbitrary(g),
                jkl: Option::arbitrary(g),
                mno: bool::arbitrary(g),
                pqr: Some(Utc::now().trunc_subsecs(3)),
                stu: Utc::now().trunc_subsecs(6),
                vwx: if vwx_arb.filter(|value| value.is_nan()).is_some() {
                    Some(0.0)
                } else {
                    vwx_arb
                },
                yza: if yza_arb.is_nan() { 0.0 } else { yza_arb },
                abcd: [1; 20],
                efgh: Option::arbitrary(g),
            })
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct TwoListLevelsWrapper(two_list_levels::TwoListLevels);

    impl Arbitrary for TwoListLevelsWrapper {
        fn arbitrary(g: &mut Gen) -> Self {
            Self(two_list_levels::TwoListLevels {
                values: Vec::arbitrary(g),
            })
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct NestedWrapper(nested::Nested);

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct FooxWrapper(nested::Foox);

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct BarWrapper(nested::Bar);

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct QuxesElementWrapper(nested::QuxesElement);

    impl Arbitrary for NestedWrapper {
        fn arbitrary(g: &mut Gen) -> Self {
            let arb: Option<FooxWrapper> = Option::arbitrary(g);
            Self(nested::Nested {
                a: u64::arbitrary(g),
                bc: Utc::now().trunc_subsecs(3),
                cde: Option::arbitrary(g),
                foox: arb.map(|value| value.0),
            })
        }
    }

    impl Arbitrary for FooxWrapper {
        fn arbitrary(g: &mut Gen) -> Self {
            let arb: BarWrapper = BarWrapper::arbitrary(g);
            Self(nested::Foox {
                fghi: String::arbitrary(g),
                bar: arb.0,
            })
        }
    }

    impl Arbitrary for BarWrapper {
        fn arbitrary(g: &mut Gen) -> Self {
            let arb: Vec<Option<QuxesElementWrapper>> = Vec::arbitrary(g);
            Self(nested::Bar {
                jklmnop: String::arbitrary(g),
                quxes: arb
                    .into_iter()
                    .map(|value| value.map(|value| value.0))
                    .collect(),
            })
        }
    }

    impl Arbitrary for QuxesElementWrapper {
        fn arbitrary(g: &mut Gen) -> Self {
            Self(nested::QuxesElement {
                xuqes: Option::arbitrary(g),
                oof: String::arbitrary(g),
            })
        }
    }
}
