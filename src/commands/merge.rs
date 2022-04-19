use crate::errors::PQRSError;
use crate::errors::PQRSError::{FileExists, FileNotFound};
use crate::utils::{check_path_present, get_row_batches, open_file, write_parquet};
use clap::Parser;
use log::debug;
use std::ops::Add;
use std::path::PathBuf;

/// Merge file(s) into another parquet file
#[derive(Parser, Debug)]
pub struct MergeCommandArgs {
    /// Parquet files to read
    #[clap(short, long, value_delimiter = ' ', multiple_values = true)]
    input: Vec<PathBuf>,

    /// Parquet file to write
    #[clap(short, long)]
    output: PathBuf,
}

pub(crate) fn execute(opts: MergeCommandArgs) -> Result<(), PQRSError> {
    debug!("The file names to read are: {:?}", opts.input);
    debug!("The file name to write to: {}", opts.output.display());

    // make sure output does not exist already before any reads
    if check_path_present(&opts.output) {
        return Err(FileExists(opts.output));
    }

    // make sure all files are present before printing any data
    for file_name in &opts.input {
        if !check_path_present(file_name) {
            return Err(FileNotFound(file_name.to_path_buf()));
        }
    }

    let seed = open_file(&opts.input[0])?;
    let mut combined = get_row_batches(seed)?;
    for input in &opts.input[1..] {
        let current = open_file(input)?;
        let local = get_row_batches(current)?;
        combined = combined.add(local);
    }
    // debug!("The combined data looks like this: {:#?}", combined);
    // debug!("This is the input schema: {:#?}", combined.schema);
    write_parquet(combined, &opts.output)?;

    Ok(())
}
