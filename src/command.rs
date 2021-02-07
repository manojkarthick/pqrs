use crate::commands::cat::CatCommand;
use crate::commands::head::HeadCommand;
use crate::commands::merge::MergeCommand;
use crate::commands::rowcount::RowCountCommand;
use crate::commands::sample::SampleCommand;
use crate::commands::schema::SchemaCommand;
use crate::commands::size::SizeCommand;
use crate::errors::PQRSError;
use clap::ArgMatches;

pub trait PQRSCommand {
    fn execute(&self) -> Result<(), PQRSError>;
}

pub fn run_command(matches: ArgMatches) -> Result<(), PQRSError> {
    match matches.subcommand() {
        ("cat", Some(m)) => CatCommand::new(m).execute(),
        ("head", Some(m)) => HeadCommand::new(m).execute(),
        ("schema", Some(m)) => SchemaCommand::new(m).execute(),
        ("rowcount", Some(m)) => RowCountCommand::new(m).execute(),
        ("size", Some(m)) => SizeCommand::new(m).execute(),
        ("sample", Some(m)) => SampleCommand::new(m).execute(),
        ("merge", Some(m)) => MergeCommand::new(m).execute(),
        _ => Ok(()),
    }
}
