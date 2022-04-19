use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, open_file};
use clap::Parser;
use log::debug;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::printer::{print_file_metadata, print_parquet_metadata};
use std::path::PathBuf;

/// Prints the schema of Parquet file(s)
#[derive(Parser, Debug)]
pub struct SchemaCommandArgs {
    /// Enable printing full file metadata
    #[clap(short = 'D', long)]
    detailed: bool,

    /// Parquet files to read
    files: Vec<PathBuf>,
}

pub(crate) fn execute(opts: SchemaCommandArgs) -> Result<(), PQRSError> {
    debug!("The file names to read are: {:?}", opts.files);
    debug!("Print Detailed output: {}", opts.detailed);

    // make sure all files are present before printing any data
    for file_name in &opts.files {
        if !check_path_present(file_name) {
            return Err(FileNotFound(file_name.to_path_buf()));
        }
    }

    for file_name in &opts.files {
        let file = open_file(file_name)?;
        match SerializedFileReader::new(file) {
            Err(e) => return Err(PQRSError::ParquetError(e)),
            Ok(parquet_reader) => {
                let metadata = parquet_reader.metadata();
                println!("Metadata for file: {}", file_name.display());
                println!();
                if opts.detailed {
                    print_parquet_metadata(&mut std::io::stdout(), metadata);
                } else {
                    print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());
                }
            }
        }
    }

    Ok(())
}
