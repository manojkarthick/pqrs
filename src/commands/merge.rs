use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::{FileExists, FileNotFound};
use crate::utils::{check_path_present, get_row_batches, write_parquet, open_file};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use std::fmt;
use std::ops::Add;

pub struct MergeCommand<'a> {
    inputs: Vec<&'a str>,
    output: &'a str,
}

impl<'a> MergeCommand<'a> {
    pub(crate) fn command() -> App<'static, 'static> {
        SubCommand::with_name("merge")
            .about("Merge file(s) into another parquet file")
            .arg(
                Arg::with_name("input")
                    .short("i")
                    .long("input")
                    .multiple(true)
                    .value_name("INPUT")
                    .value_delimiter(" ")
                    .required(true)
                    .help("Parquet files to read"),
            )
            .arg(
                Arg::with_name("output")
                    .short("o")
                    .long("output")
                    .value_name("OUTPUT")
                    .required(true)
                    .help("Parquet file to write"),
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            inputs: matches.values_of("input").unwrap().collect(),
            output: matches.value_of("output").unwrap(),
        }
    }
}

impl<'a> PQRSCommand for MergeCommand<'a> {
    fn execute(&self) -> Result<(), PQRSError> {
        // print debugging information
        debug!("{:#?}", self);

        // make sure output does not exist already before any reads
        if check_path_present(self.output) {
            return Err(FileExists(self.output.to_string()));
        }

        // make sure all files are present before printing any data
        for file_name in &self.inputs {
            if !check_path_present(*file_name) {
                return Err(FileNotFound(String::from(*file_name)));
            }
        }

        let seed = open_file(self.inputs[0])?;
        let mut combined = get_row_batches(seed)?;
        for input in &self.inputs[1..] {
            let current = open_file(input)?;
            let local = get_row_batches(current)?;
            combined = combined.add(local);
        }
        // debug!("The combined data looks like this: {:#?}", combined);
        // debug!("This is the input schema: {:#?}", combined.schema);
        write_parquet(combined, self.output)?;

        Ok(())
    }
}

impl<'a> fmt::Debug for MergeCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "The file names to read are: {}", self.inputs.join(", "))?;
        writeln!(f, "The file name to write to: {}", self.output)?;

        Ok(())
    }
}
