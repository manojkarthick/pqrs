use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file, print_rows};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use std::fmt;

pub struct HeadCommand<'a> {
    file_name: &'a str,
    num_records: i64,
    use_json: bool,
}

impl<'a> HeadCommand<'a> {
    pub(crate) fn command() -> App<'static, 'static> {
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
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            file_name: matches.value_of("file").unwrap(),
            num_records: matches.value_of("records").unwrap().parse().unwrap(),
            use_json: matches.is_present("json"),
        }
    }
}

impl<'a> PQRSCommand for HeadCommand<'a> {
    fn execute(&self) -> Result<(), PQRSError> {
        // print debugging information
        debug!("{:#?}", self);

        if !check_path_present(self.file_name) {
            return Err(FileNotFound(String::from(self.file_name)));
        }

        let file = open_file(self.file_name)?;
        print_rows(file, Some(self.num_records), self.use_json)?;

        Ok(())
    }
}

impl<'a> fmt::Debug for HeadCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "The file name to read is: {}", &self.file_name)?;
        writeln!(f, "Number of records to print: {}", &self.num_records)?;
        writeln!(f, "Use JSON Output format: {}", &self.use_json)?;

        Ok(())
    }
}
