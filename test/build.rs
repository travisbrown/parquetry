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
