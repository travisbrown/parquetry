use cli_helpers::prelude::*;
use std::{fs::File, path::PathBuf};

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::Gen { source } => {
            let schema = parquetry_gen::ParsedFileSchema::open(source, Default::default())?;
            println!("{}", schema.code()?);
        }
        Command::Dump { input } => {
            let reader =
                parquet::file::serialized_reader::SerializedFileReader::new(File::open(input)?)?;
            for row in reader {
                let row = row?;
                for (name, field) in row.get_column_iter() {
                    println!("{name}: {field:?}");
                }
            }
        }
        Command::DumpSchema { source } => {
            let schema = parquetry_gen::ParsedFileSchema::open(source, Default::default())?;
            println!("{:?}", schema.schema);
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Code generation error")]
    Parquetry(#[from] parquetry_gen::error::Error),
    #[error("CLI argument reading error")]
    Args(#[from] cli_helpers::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "parquetry", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Gen {
        #[clap(long)]
        source: PathBuf,
    },
    Dump {
        #[clap(long)]
        input: PathBuf,
    },
    DumpSchema {
        #[clap(long)]
        source: PathBuf,
    },
}
