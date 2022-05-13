use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, get_row_count, open_file};
use clap::Parser;
use log::debug;
use std::path::PathBuf;

/// Prints the count of rows in Parquet file(s)
#[derive(Parser, Debug)]
pub struct RowCountCommandArgs {
    /// Parquet files to read
    files: Vec<PathBuf>,
}

pub(crate) fn execute(opts: RowCountCommandArgs) -> Result<(), PQRSError> {
    debug!("The file names to read are: {:?}", opts.files);

    // make sure all files are present before printing any data
    for file_name in &opts.files {
        if !check_path_present(file_name) {
            return Err(FileNotFound(file_name.to_path_buf()));
        }
    }

    for file_name in &opts.files {
        let file = open_file(file_name)?;
        let row_count = get_row_count(file)?;
        println!("File Name: {}: {} rows", file_name.display(), &row_count);
    }

    Ok(())
}
