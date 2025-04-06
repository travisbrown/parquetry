# Parquet code generation for Rust

[![Rust build status](https://img.shields.io/github/actions/workflow/status/travisbrown/parquetry/ci.yaml?branch=main)](https://github.com/travisbrown/parquetry/actions)
[![Coverage status](https://img.shields.io/codecov/c/github/travisbrown/parquetry/main.svg)](https://codecov.io/github/travisbrown/parquetry)

This project provides tools for generating Rust code to work with [Parquet][parquet] files using the Rust implementation of [Arrow][arrow].
It includes both a code generation crate (`parquetry-gen`) and a small runtime library required by the generated code (`parquetry`).

Please note that this software is **not** "open source",
but the source is available for use and modification by individuals, non-profit organizations, and worker-owned businesses
(see the [license section](#license) below for details).

## Table of contents

* [Example](#example)
* [Dependencies](#dependencies)
* [Usage](#usage)
* [Testing](#testing)
* [Status and scope](#status-and-scope)
* [Warnings](#warnings)
* [License](#license)

## Example

Given a schema like this:

```
message user {
    required int64 id (integer(64, false));
    required int64 ts (timestamp(millis, true));
    optional int32 status;

    optional group user_info {
        required byte_array screen_name (string);

        optional group user_name_info {
            required byte_array name (string);

            optional group user_profile_info {
                required int64 created_at (timestamp(millis, true));
                required byte_array location (string);
                required byte_array description (string);
                optional byte_array url (string);

                required int32 followers_count;
                required int32 friends_count;
                required int32 favourites_count;
                required int32 statuses_count;

                optional group withheld_in_countries (list) {
                    repeated group list {
                        required byte_array element (string);
                    }
                }
            }
        }
    }
}
```

The code generator will produce the following Rust structs:

```rust
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct User {
    pub id: u64,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub ts: chrono::DateTime<chrono::Utc>,
    pub status: Option<i32>,
    pub user_info: Option<UserInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UserInfo {
    pub screen_name: String,
    pub user_name_info: Option<UserNameInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UserNameInfo {
    pub name: String,
    pub user_profile_info: Option<UserProfileInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UserProfileInfo {
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub location: String,
    pub description: String,
    pub url: Option<String>,
    pub followers_count: i32,
    pub friends_count: i32,
    pub favourites_count: i32,
    pub statuses_count: i32,
    pub withheld_in_countries: Option<Vec<String>>,
}
```

It will also generate an instance of the `parquetry::Schema` trait for `User` with the code for reading and writing values to Parquet files.

## Dependencies

All usage requires the use of the `parquetry`, [`parquet`][rust-parquet], [`chrono`][chrono], and [`lazy_static`][lazy-static] crates as runtime dependencies.

If the `serde_support` flag is enabled in configuration (which it is by default),
you will also need a dependency on [`serde`][serde] with the `derive` feature enabled.

If the `tests` flag is enabled in configuration (also the default),
you will need to add [`bincode`][bincode] (with the `serde` feature enabled),
[`tempfile`][tempfile], and [`quickcheck`][quickcheck] to your `dev-dependencies`.

## Usage

The `example` directory provides a fairly minimal example, and the generated code is checked in there.
In most cases a `build.rs` like the following should be all you need:

```rust
use std::{fs::File, io::Write};

fn main() -> Result<(), parquetry_gen::error::Error> {
    for schema in parquetry_gen::ParsedFileSchema::open_dir(
        "src/schemas/",
        Default::default(),
        Some(".parquet.txt"),
    )? {
        println!("cargo:rerun-if-changed={}", schema.absolute_path_str()?);
        let mut output = File::create(format!("src/{}.rs", schema.name))?;
        write!(output, "{}", schema.code()?)?;
    }

    Ok(())
}
```

By default the generated code is formatted with [`prettyplease`][prettyplease] and is annotated to indicate that it should not be formatted by Rustfmt,
but if you'd prefer to use Rustfmt yourself, you can set `format` to false in the configuration.

## Testing

The default configuration will generate test code that uses [QuickCheck][quickcheck] to generate arbitrary values and confirm that they serialize and deserialize correctly.

The test code generation currently does not support some types.
Specifically, sequences of floating point numbers, fixed-length byte arrays, and timestamps are not supported.
If your schema contains any of these types, the generated test code will simply not compile, and you will have to disable the `tests` flag in your configuration
(you can also open an issue if you'd like).

The generated test code does not produce `NaN` values for floating point types.
If you want to confirm that your system handles these values correctly, you'll have to do that manually.

By default the generated arbitrary values for your types may be very large.
You may want to set the `QUICKCHECK_GENERATOR_SIZE` environment variable to a small value (e.g. `10` or `20`) if your tests are too slow.

## Status and scope

These tools support schemas with most physical and logical types, and with arbitrary nestings of lists, optional fields, and structures.

Missing features that I might add at some point:

* 8- and 16-bit logical integer types (trivial, I just haven't needed them)
* `DATE`, `TIME`, `INTERVAL`, and `UUID` (same as previous)
* `DECIMAL` (at least for the `INT32` and `INT64` representations)
* `ENUM` (not really useful in this context since the schema doesn't enumerate the variants?)
* Maps (a little more work but probably worth having)

Features that will probably never be supported:

* The `INT96` physical type (which has been [deprecated](https://issues.apache.org/jira/browse/PARQUET-323))
* Timestamps with [local semantics](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#local-semantics-timestamps-not-normalized-to-utc) (`isAdjustedToUTC = false`)
* [Legacy list shapes](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules)
* [Legacy map shapes](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules-1)
* A way to avoid the [`chrono`][chrono] dependency or support for other time libraries

This project differs from [`parquet_derive`][parquet-derive] in a few ways:

* Both generate reading and writing code, but this project generates Rust structs from the schema, as opposed to the reverse.
* This project does not use `parquet::record::RecordWriter` (it just didn't seem all that useful and I wanted more flexibility).
* This project supports nested structures.

In general the two projects have different use cases, and if you just want to store some Rust values in Parquet, I'd recommend choosing `parquet_derive`.

## Warnings

### Name collisions

There's currently no special handling for field names that collide with Rust keywords, names from the standard library, etc.
Group names should also be unique within a schema.
The code generator also produces non-public structs with names that could in theory collide with user-generated code.

In most of these cases the problems should be immediately obvious, since the generated code will generally just not compile.
It wouldn't be hard to check for these collisions and provide better errors, or to allow more user control over naming to avoid these issues,
but this hasn't been a priority for me.

### Constructors

The generated code includes `fn new` constructors for each struct that will truncate the precision of any `DateTime<Utc>` to the number of subsecond digits
supported by the column representation. These constructors will also check whether any string arguments contain null bytes, and will return an error if they do.

If you don't want either behavior, you can construct the structs manually, as all fields are always public.

### Serde instances

By default the generated code will include derived Serde instances for serialization.
These instances will use the time unit specified by the schema (millisecond or microsecond) for fields with the type `DateTime<Utc>` or `Option<DateTime<Utc>>`, 
but fields of type `Vec<DateTime<Utc>>` will use the default Serde serialization encoding for `DateTime<Utc>`.

There's no particular reason for this beyond the fact that [`chrono::serde`][chrono-serde] only provides e.g. `ts_milliseconds` and `ts_milliseconds_option` functions,
and the runtime library could easily provide its own `ts_milliseconds_vec` that would be used in these cases.

### Performance

I haven't made any attempt to optimize the generated code and probably won't until performance becomes an issue for my use cases.

## License

This software is published under the [Anti-Capitalist Software License][acsl] (v. 1.4).

[acsl]: https://anticapitalist.software/
[arrow]: https://arrow.apache.org/
[arrow-rs]: https://github.com/apache/arrow-rs
[bincode]: https://docs.rs/bincode/latest/bincode/
[chrono]: https://docs.rs/chrono/latest/chrono/
[chrono-serde]: https://docs.rs/chrono/latest/chrono/serde/index.html
[lazy-static]: https://docs.rs/lazy_static/latest/lazy_static/
[parquet]: https://parquet.apache.org/
[parquet-derive]: https://crates.io/crates/parquet_derive
[prettyplease]: https://github.com/dtolnay/prettyplease
[quickcheck]: https://docs.rs/quickcheck/latest/quickcheck/
[rust-parquet]: https://docs.rs/parquet/latest/parquet/
[serde]: https://serde.rs/
[tempfile]: https://docs.rs/tempfile/latest/tempfile/
