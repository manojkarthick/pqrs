use clap::AppSettings::ArgRequiredElseHelp;
use clap::{crate_authors, crate_version, App, Arg};
use env_logger::Env;

use crate::errors::PQRSError;

mod command;
mod commands;
mod errors;
mod utils;

fn main() -> Result<(), PQRSError> {
    let matches = App::new("pqrs")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Apache Parquet command-line utility")
        .setting(ArgRequiredElseHelp)
        .arg(
            Arg::with_name("debug")
                .short("d")
                .long("debug")
                .takes_value(false)
                .global(true)
                .help("Show debug output"),
        )
        .subcommands(vec![
            commands::cat::CatCommand::command(),
            commands::schema::SchemaCommand::command(),
            commands::head::HeadCommand::command(),
            commands::rowcount::RowCountCommand::command(),
            commands::size::SizeCommand::command(),
            commands::sample::SampleCommand::command(),
            commands::merge::MergeCommand::command(),
        ])
        .get_matches();

    // initialize logger for the app and set logging level to info if no environment variable present
    let mut env = Env::default();
    // if --debug flag is used, then set logging level to debug
    if matches.is_present("debug") {
        env = env.default_filter_or("debug");
    } else {
        env = env.default_filter_or("info");
    }

    env_logger::Builder::from_env(env).init();

    // run the right subcommand
    command::run_command(matches)?;

    Ok(())
}
