use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file, print_rows_random};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use std::fmt;

pub struct SampleCommand<'a> {
    file_name: &'a str,
    num_records: i64,
    use_json: bool,
    randomize: bool,
}

impl<'a> SampleCommand<'a> {
    pub(crate) fn command() -> App<'static, 'static> {
        SubCommand::with_name("sample")
            .about("Prints a random sample of records from the Parquet file")
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
                    .takes_value(true)
                    .required(true)
                    .help("The number of records to sample"),
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            file_name: matches.value_of("file").unwrap(),
            num_records: matches.value_of("records").unwrap().parse().unwrap(),
            use_json: matches.is_present("json"),
            randomize: true,
        }
    }
}

impl<'a> PQRSCommand for SampleCommand<'a> {
    fn execute(&self) -> Result<(), PQRSError> {
        // print debugging information
        debug!("{:#?}", self);

        if !check_path_present(self.file_name) {
            return Err(FileNotFound(String::from(self.file_name)));
        }

        let file = open_file(self.file_name)?;
        print_rows_random(file, self.num_records, self.use_json)?;

        Ok(())
    }
}

impl<'a> fmt::Debug for SampleCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "The file name to read is: {}", &self.file_name)?;
        writeln!(f, "Number of records to print: {}", &self.num_records)?;
        writeln!(f, "Use JSON Output format: {}", &self.use_json)?;
        writeln!(f, "Randomize output: {}", self.randomize)?;

        Ok(())
    }
}
