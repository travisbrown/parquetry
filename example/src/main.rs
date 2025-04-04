use chrono::Utc;
use cli_helpers::prelude::*;
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquetry::Schema;
use std::fs::File;
use std::path::PathBuf;

#[allow(dead_code)]
mod user;
use user::*;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::Read { input } => {
            for value in User::read(File::open(input)?, ReadOptionsBuilder::default().build()) {
                let value = value?;
                println!("{:?}", value);
            }
        }
        Command::Dump { output } => {
            let data = vec![
                User {
                    id: 1237981,
                    ts: Utc::now(),
                    status: None,
                    user_info: None,
                },
                User {
                    id: 1237981123123,
                    ts: Utc::now(),
                    status: None,
                    user_info: Some(UserInfo {
                        screen_name: "foo".to_string(),
                        user_name_info: None,
                    }),
                },
                User {
                    id: 1237981123123,
                    ts: Utc::now(),
                    status: None,
                    user_info: Some(UserInfo {
                        screen_name: "foo".to_string(),
                        user_name_info: None,
                    }),
                },
                User {
                    id: u64::MAX,
                    ts: Utc::now(),
                    status: None,
                    user_info: Some(UserInfo {
                        screen_name: "foo".to_string(),
                        user_name_info: Some(UserNameInfo {
                            name: "Foo McBar".to_string(),
                            user_profile_info: Some(UserProfileInfo {
                                created_at: Utc::now(),
                                created_at_date: None,
                                location: "Wherever".to_string(),
                                description: "Whatever".to_string(),
                                url: Some("https://foo.bar/".to_string()),
                                followers_count: 1028,
                                friends_count: 12376182,
                                favourites_count: 10,
                                statuses_count: 0,
                                withheld_in_countries: None,
                            }),
                        }),
                    }),
                },
                User {
                    id: 1237981123123127,
                    ts: Utc::now(),
                    status: None,
                    user_info: Some(UserInfo {
                        screen_name: "foo".to_string(),
                        user_name_info: Some(UserNameInfo {
                            name: "Foo McBar".to_string(),
                            user_profile_info: Some(UserProfileInfo {
                                created_at: Utc::now(),
                                created_at_date: Some(Utc::now().date_naive()),
                                location: "Wherever".to_string(),
                                description: "Whatever".to_string(),
                                url: Some("https://foo.bar/".to_string()),
                                followers_count: 1028,
                                friends_count: 12376182,
                                favourites_count: 10,
                                statuses_count: 0,
                                withheld_in_countries: Some(vec![
                                    "de".to_string(),
                                    "fr".to_string(),
                                ]),
                            }),
                        }),
                    }),
                },
                User {
                    id: 1237981123123127,
                    ts: Utc::now(),
                    status: Some(63),
                    user_info: None,
                },
            ];

            let output = File::create(output)?;

            User::write_row_groups(output, Default::default(), vec![data])?;
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Parquetry error")]
    Parquetry(#[from] parquetry::error::Error),
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
    Dump {
        #[clap(long)]
        output: PathBuf,
    },
    Read {
        #[clap(long)]
        input: PathBuf,
    },
}
