use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, get_pretty_size, get_size, open_file};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use std::fmt;

pub struct SizeCommand<'a> {
    file_names: Vec<&'a str>,
    compressed: bool,
    pretty: bool,
}

impl<'a> SizeCommand<'a> {
    pub(crate) fn command() -> App<'static, 'static> {
        SubCommand::with_name("size")
            .about("Prints the size of Parquet file(s)")
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
                Arg::with_name("compressed")
                    .long("compressed")
                    .short("c")
                    .takes_value(false)
                    .required(false)
                    .help("Show compressed size"),
            )
            .arg(
                Arg::with_name("pretty")
                    .long("pretty")
                    .short("p")
                    .takes_value(false)
                    .required(false)
                    .help("Show pretty, human readable size"),
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            file_names: matches.values_of("files").unwrap().collect(),
            compressed: matches.is_present("compressed"),
            pretty: matches.is_present("pretty"),
        }
    }
}

impl<'a> PQRSCommand for SizeCommand<'a> {
    fn execute(&self) -> Result<(), PQRSError> {
        // print debugging information
        debug!("{:#?}", self);

        // make sure all files are present before printing any data
        for file_name in &self.file_names {
            if !check_path_present(*file_name) {
                return Err(FileNotFound(String::from(*file_name)));
            }
        }

        println!("Size in Bytes:");
        for file_name in &self.file_names {
            let file = open_file(*file_name)?;
            let size_info = get_size(file)?;

            println!();
            println!("File Name: {}", &file_name);

            if !self.compressed {
                if self.pretty {
                    println!("Uncompressed Size: {}", get_pretty_size(size_info.0));
                } else {
                    println!("Uncompressed Size: {}", size_info.0);
                }
            } else {
                if self.pretty {
                    println!("Compressed Size: {}", get_pretty_size(size_info.1));
                } else {
                    println!("Compressed Size: {}", size_info.1);
                }
            }
        }

        Ok(())
    }
}

impl<'a> fmt::Debug for SizeCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "The file names to read are: {}",
            &self.file_names.join(", ")
        )
    }
}
