use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, get_row_count, open_file};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use std::fmt;

pub struct RowCountCommand<'a> {
    file_names: Vec<&'a str>,
}

impl<'a> RowCountCommand<'a> {
    pub(crate) fn command() -> App<'static, 'static> {
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
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            file_names: matches.values_of("files").unwrap().collect(),
        }
    }
}

impl<'a> PQRSCommand for RowCountCommand<'a> {
    fn execute(&self) -> Result<(), PQRSError> {
        // print debugging information
        debug!("{:#?}", self);

        // make sure all files are present before printing any data
        for file_name in &self.file_names {
            if !check_path_present(*file_name) {
                return Err(FileNotFound(String::from(*file_name)));
            }
        }

        for file_name in &self.file_names {
            let file = open_file(file_name)?;
            let row_count = get_row_count(file)?;
            println!("File Name: {}: {} rows", &file_name, &row_count);
        }

        Ok(())
    }
}

impl<'a> fmt::Debug for RowCountCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "The file names to read are: {}",
            self.file_names.join(", ")
        )
    }
}
