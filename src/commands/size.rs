use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, get_pretty_size, get_size, open_file};
use clap::Parser;
use log::debug;
use std::path::PathBuf;

/// Prints the size of Parquet file(s)
#[derive(Parser, Debug)]
pub struct SizeCommandArgs {
    /// Show pretty, human readable size
    #[arg(short, long)]
    pretty: bool,

    /// Show compressed size
    #[arg(short, long)]
    compressed: bool,

    /// Parquet files to read
    files: Vec<PathBuf>,
}

pub(crate) fn execute(opts: SizeCommandArgs) -> Result<(), PQRSError> {
    debug!("The file names to read are: {:?}", opts.files);

    // make sure all files are present before printing any data
    for file_name in &opts.files {
        if !check_path_present(file_name) {
            return Err(FileNotFound(file_name.to_path_buf()));
        }
    }

    println!("Size in Bytes:");
    for file_name in &opts.files {
        let file = open_file(file_name)?;
        let size_info = get_size(file)?;

        println!();
        println!("File Name: {}", file_name.display());

        if !opts.compressed {
            if opts.pretty {
                println!("Uncompressed Size: {}", get_pretty_size(size_info.0));
            } else {
                println!("Uncompressed Size: {}", size_info.0);
            }
        } else if opts.pretty {
            println!("Compressed Size: {}", get_pretty_size(size_info.1));
        } else {
            println!("Compressed Size: {}", size_info.1);
        }
    }

    Ok(())
}
