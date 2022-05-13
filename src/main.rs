use clap::{Parser, Subcommand};

use crate::errors::PQRSError;

mod commands;
mod errors;
mod utils;

#[derive(Subcommand, Debug)]
enum Commands {
    Cat(commands::cat::CatCommandArgs),
    Head(commands::head::HeadCommandArgs),
    Merge(commands::merge::MergeCommandArgs),
    #[clap(alias = "rowcount")]
    RowCount(commands::rowcount::RowCountCommandArgs),
    Sample(commands::sample::SampleCommandArgs),
    Schema(commands::schema::SchemaCommandArgs),
    Size(commands::size::SizeCommandArgs),
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Show debug output
    #[clap(short, long)]
    debug: bool,

    #[clap(subcommand)]
    command: Commands,
}

fn main() -> Result<(), PQRSError> {
    let args = Args::parse();

    if args.debug {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    log::debug!("args: {:?}", args);

    match args.command {
        Commands::Cat(opts) => commands::cat::execute(opts)?,
        Commands::Head(opts) => commands::head::execute(opts)?,
        Commands::Merge(opts) => commands::merge::execute(opts)?,
        Commands::RowCount(opts) => commands::rowcount::execute(opts)?,
        Commands::Sample(opts) => commands::sample::execute(opts)?,
        Commands::Schema(opts) => commands::schema::execute(opts)?,
        Commands::Size(opts) => commands::size::execute(opts)?,
    }

    Ok(())
}
