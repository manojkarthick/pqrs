use crate::command::PQRSCommand;
use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::printer::{print_file_metadata, print_parquet_metadata};
use std::fmt;

pub struct SchemaCommand<'a> {
    file_names: Vec<&'a str>,
    use_detailed: bool,
}

impl<'a> SchemaCommand<'a> {
    pub(crate) fn command() -> App<'static, 'static> {
        SubCommand::with_name("schema")
            .about("Prints the schema of Parquet file(s)")
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
                Arg::with_name("detailed")
                    .long("detailed")
                    .short("D")
                    .takes_value(false)
                    .required(false)
                    .help("Enable printing full file metadata"),
            )
    }

    pub(crate) fn new(matches: &'a ArgMatches<'a>) -> Self {
        Self {
            file_names: matches.values_of("files").unwrap().collect(),
            use_detailed: matches.is_present("detailed"),
        }
    }
}

impl<'a> PQRSCommand for SchemaCommand<'a> {
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
            match SerializedFileReader::new(file) {
                Err(e) => return Err(PQRSError::ParquetError(e)),
                Ok(parquet_reader) => {
                    let metadata = parquet_reader.metadata();
                    println!("Metadata for file: {}", &file_name);
                    println!();
                    if self.use_detailed {
                        print_parquet_metadata(&mut std::io::stdout(), &metadata);
                    } else {
                        print_file_metadata(
                            &mut std::io::stdout(),
                            &metadata.file_metadata(),
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

impl<'a> fmt::Debug for SchemaCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "The file names to read are: {}",
            &self.file_names.join(", ")
        )?;
        writeln!(f, "Print Detailed output: {}", &self.use_detailed)?;

        Ok(())
    }
}
