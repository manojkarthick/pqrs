use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file, print_rows_random, Formats};
use clap::Parser;
use log::debug;
use std::path::PathBuf;

/// Prints a random sample of records from the Parquet file
#[derive(Parser, Debug)]
pub struct SampleCommandArgs {
    /// Use JSON lines format for printing
    #[arg(short, long)]
    json: bool,

    /// The number of records to sample
    #[arg(short = 'n', long)]
    records: usize,

    /// Parquet file to read
    file: PathBuf,
}

pub(crate) fn execute(opts: SampleCommandArgs) -> Result<(), PQRSError> {
    let format = if opts.json {
        Formats::Json
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
    print_rows_random(file, opts.records, format)?;

    Ok(())
}
