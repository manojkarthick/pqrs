use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file, print_rows, Formats};
use clap::Parser;
use log::debug;
use std::path::PathBuf;

/// Prints the first n records of the Parquet file
#[derive(Parser, Debug)]
pub struct HeadCommandArgs {
    /// Use CSV format for printing
    #[arg(short, long, conflicts_with = "json")]
    csv: bool,

    /// Use JSON lines format for printing
    #[arg(short, long, conflicts_with = "csv")]
    json: bool,

    /// The number of records to show (default: 5)
    #[arg(short = 'n', long, default_value = "5")]
    records: usize,

    /// Parquet file to read
    file: PathBuf,
}

pub(crate) fn execute(opts: HeadCommandArgs) -> Result<(), PQRSError> {
    let format = if opts.json {
        Formats::Json
    } else if opts.csv {
        Formats::Csv
    } else {
        Formats::Default
    };

    debug!("The file name to read is: {}", opts.file.display());
    debug!("Number of records to print: {}", opts.records);
    debug!("Use Output format: {}", format);

    if !check_path_present(&opts.file) {
        return Err(FileNotFound(opts.file));
    }

    let file = open_file(&opts.file)?;
    print_rows(file, Some(opts.records), format)?;

    Ok(())
}
