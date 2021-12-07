use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file, print_rows, is_hidden};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use std::fmt;
use std::fs::metadata;
use walkdir::WalkDir;
use std::collections::HashSet;
use crate::utils::Formats;

/// The config params for the "cat" subcommand
pub struct CatCommand<'a> {
    locations: Vec<&'a str>,
    format: &'a Formats,
}

impl<'a> CatCommand<'a> {
    /// Return the clap subcommand definition
    pub(crate) fn command() -> App<'static, 'static> {
        SubCommand::with_name("cat")
            .about("Prints the contents of Parquet file(s)")
            .arg(
                Arg::with_name("locations")
                    .index(1)
                    .multiple(true)
                    .value_name("LOCATIONS")
                    .value_delimiter(" ")
                    .required(true)
                    .help("Parquet files or folders to read from"),
            )
            .arg(
                Arg::with_name("json")
                    .long("json")
                    .short("j")
                    .takes_value(false)
                    .required(false)
                    .conflicts_with("csv")
                    .help("Use JSON lines format for printing"),
            )
            .arg(
                Arg::with_name("csv")
                    .long("csv")
                    .short("c")
                    .takes_value(false)
                    .required(false)
                    .conflicts_with("json")
                    .help("Use CSV format for printing")
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            locations: matches.values_of("locations").unwrap().collect(),
            format: if matches.is_present("json") {
                &Formats::Json
            } else if matches.is_present("csv") {
                &Formats::Csv
            } else {
                &Formats::Default
            },
        }
    }
}

impl<'a> PQRSCommand for CatCommand<'a> {
    fn execute(&self) -> Result<(), PQRSError> {
        // print debugging information
        debug!("{:#?}", self);

        let mut directories = vec![];
        let mut files = HashSet::new();
        for location in &self.locations {
            let meta = metadata(location).unwrap();
            if meta.is_dir() {
                directories.push(String::from(*location));
            }
            if meta.is_file() {
                files.insert(String::from(*location));
            }
        }

        for directory in &directories {
            let walker = WalkDir::new(directory).into_iter();
            for entry in walker.filter_entry(|e| !is_hidden(e)).filter_map(|e| e.ok()) {
                debug!("{}", entry.path().display());
                let path = String::from(entry.path().to_str().unwrap());
                let meta = metadata(&path).unwrap();
                if meta.is_file() {
                    files.insert(path);
                }
            }
        }

        // find all the files after walking the directories
        debug!("The files are: {:#?}", files);

        // make sure all files are present before printing any data
        for file_name in &files {
            if !check_path_present(file_name.as_ref()) {
                return Err(FileNotFound(String::from(file_name)));
            }
        }

        for file_name in &files {
            let file = open_file(file_name)?;
            let info_string = format!("File: {}", file_name);
            let length = info_string.len();
            eprintln!("\n{}", "#".repeat(length));
            eprintln!("{}", info_string);
            eprintln!("{}\n", "#".repeat(length));
            print_rows(file, None, self.format)?;
        }

        Ok(())
    }
}

impl<'a> fmt::Debug for CatCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "The locations to read from are: {}",
            &self.locations.join(", ")
        )?;
        writeln!(f, "Using Output format: {}", self.format.to_string())?;

        Ok(())
    }
}
