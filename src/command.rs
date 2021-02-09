use crate::commands::cat::CatCommand;
use crate::commands::head::HeadCommand;
use crate::commands::merge::MergeCommand;
use crate::commands::rowcount::RowCountCommand;
use crate::commands::sample::SampleCommand;
use crate::commands::schema::SchemaCommand;
use crate::commands::size::SizeCommand;
use crate::errors::PQRSError;
use clap::ArgMatches;

/// The trait to be implemented by every subcommand in the binary
pub trait PQRSCommand {
    /// The execute method runs the command and return errors, if any
    fn execute(&self) -> Result<(), PQRSError>;
}

/// Run the appropriate subcommand based on the input from the user.
pub fn run_command(matches: ArgMatches) -> Result<(), PQRSError> {
    // match based on the subcommand name, create an instance and execute the command
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
