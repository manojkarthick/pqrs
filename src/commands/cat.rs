use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file, print_rows};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use std::fmt;

pub struct CatCommand<'a> {
    file_names: Vec<&'a str>,
    use_json: bool,
}

impl<'a> CatCommand<'a> {
    pub(crate) fn command() -> App<'static, 'static> {
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
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            file_names: matches.values_of("files").unwrap().collect(),
            use_json: matches.is_present("json"),
        }
    }
}

impl<'a> PQRSCommand for CatCommand<'a> {
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
            print_rows(file, None, self.use_json)?;
        }

        Ok(())
    }
}

impl<'a> fmt::Debug for CatCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "The file names to read are: {}",
            &self.file_names.join(", ")
        )?;
        writeln!(f, "Use JSON Output format: {}", &self.use_json)?;

        Ok(())
    }
}
