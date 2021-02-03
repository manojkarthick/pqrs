use clap::{crate_authors, crate_version, App, Arg, SubCommand};
use env_logger::Env;

use crate::commands::run_command;
use crate::errors::PQRSError;

mod commands;
mod errors;
mod utils;

fn main() -> Result<(), PQRSError> {
    // initialize logger for the app and set logging level to info if no environment variable present
    let env = Env::default().default_filter_or("debug");
    env_logger::Builder::from_env(env).init();

    let matches = App::new("pqrs")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Apache Parquet command-line utility")
        .subcommand(
            SubCommand::with_name("cat")
                .about("Prints the contents of Parquet file(s)")
                .arg(
                    Arg::with_name("files")
                        .index(1)
                        .multiple(true)
                        .value_name("FILES")
                        .value_delimiter(" ")
                        .required(true)
                        .help("Parquet files to read"),
                )
                .arg(
                    Arg::with_name("json")
                        .long("json")
                        .short("j")
                        .takes_value(false)
                        .required(false)
                        .help("Use JSON lines format for printing"),
                ),
        )
        .subcommand(
            SubCommand::with_name("schema")
                .about("Prints the schema of Parquet file(s)")
                .arg(
                    Arg::with_name("files")
                        .index(1)
                        .multiple(true)
                        .value_name("FILES")
                        .value_delimiter(" ")
                        .required(true)
                        .help("Parquet files to read"),
                )
                .arg(
                    Arg::with_name("detailed")
                        .long("detailed")
                        .short("d")
                        .takes_value(false)
                        .required(false)
                        .help("Enable printing full file metadata"),
                ),
        )
        .subcommand(
            SubCommand::with_name("head")
                .about("Prints the first n records of the Parquet file")
                .arg(
                    Arg::with_name("file")
                        .index(1)
                        .value_name("FILE")
                        .required(true)
                        .help("Parquet file to read"),
                )
                .arg(
                    Arg::with_name("json")
                        .long("json")
                        .short("j")
                        .takes_value(false)
                        .required(false)
                        .help("Use JSON lines format for printing"),
                )
                .arg(
                    Arg::with_name("records")
                        .long("records")
                        .short("n")
                        .default_value("5")
                        .takes_value(true)
                        .required(false)
                        .help("The number of records to show (default: 5)"),
                ),
        )
        .subcommand(
            SubCommand::with_name("rowcount")
                .about("Prints the count of rows in Parquet file(s)")
                .arg(
                    Arg::with_name("files")
                        .index(1)
                        .multiple(true)
                        .value_name("FILES")
                        .value_delimiter(" ")
                        .required(true)
                        .help("Parquet files to read"),
                ),
        )
        .subcommand(
            SubCommand::with_name("size")
                .about("Prints the size of Parquet file(s)")
                .arg(
                    Arg::with_name("files")
                        .index(1)
                        .multiple(true)
                        .value_name("FILES")
                        .value_delimiter(" ")
                        .required(true)
                        .help("Parquet files to read"),
                ),
        )
        .get_matches();

    run_command(matches)?;

    Ok(())
}
