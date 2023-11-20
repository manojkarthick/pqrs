use crate::errors::PQRSError;
use crate::errors::PQRSError::{FileExists, FileNotFound};
use crate::utils::{check_path_present, get_row_batches, open_file, write_parquet};
use clap::Parser;
use log::debug;
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::ops::Add;
use std::path::PathBuf;

/// Merge file(s) into another parquet file
#[derive(Parser, Debug)]
pub struct MergeCommandArgs {
    /// Parquet files to read
    #[arg(short, long, value_delimiter = ' ', num_args = 1..)]
    input: Vec<PathBuf>,

    /// Parquet file to write
    #[arg(short, long)]
    output: PathBuf,

    /// Path to a json config file specifying WriterProperties::builder() properties.
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MergeConfig {
    pub set_dictionary_enabled: Option<bool>,
    /// The encodings for this are the just text values of the enum parquet::basic::Encoding
    pub column_encodings: Option<HashMap<String, String>>,
    pub column_dictionary_enabled: Option<HashMap<String, bool>>,
    pub compression: Option<String>,
    pub compression_level: Option<u32>,
}

fn build_encoding_mappings() -> HashMap<&'static str, Encoding> {
    HashMap::from([
        ("PLAIN", Encoding::PLAIN),
        ("PLAIN_DICTIONARY", Encoding::PLAIN_DICTIONARY),
        ("RLE", Encoding::RLE),
        ("BIT_PACKED", Encoding::BIT_PACKED),
        ("DELTA_BINARY_PACKED", Encoding::DELTA_BINARY_PACKED),
        ("DELTA_LENGTH_BYTE_ARRAY", Encoding::DELTA_LENGTH_BYTE_ARRAY),
        ("DELTA_BYTE_ARRAY", Encoding::DELTA_BYTE_ARRAY),
        ("RLE_DICTIONARY", Encoding::RLE_DICTIONARY),
        ("BYTE_STREAM_SPLIT", Encoding::BYTE_STREAM_SPLIT),
    ])
}

fn build_props_from_json_config(
    config_path: PathBuf,
) -> Result<WriterProperties, PQRSError> {
    let data = fs::read_to_string(config_path)?;
    let merge_config: MergeConfig = serde_json::from_str(&data)?;
    let mut props =
        WriterProperties::builder().set_writer_version(WriterVersion::PARQUET_2_0);

    if let Some(de) = merge_config.set_dictionary_enabled {
        props = props.set_dictionary_enabled(de);
    }

    if let Some(column_encodings) = merge_config.column_encodings {
        let encoding_mappings = build_encoding_mappings();
        for (column_name, encoding_str) in column_encodings {
            if !encoding_mappings.contains_key(encoding_str.as_str()) {
                return Err(PQRSError::IllegalEncodingType());
            }

            let encoding = *encoding_mappings
                .get(encoding_str.clone().as_str())
                .unwrap();
            props = props.set_column_encoding(ColumnPath::from(column_name), encoding)
        }
    }

    if let Some(column_de) = merge_config.column_dictionary_enabled {
        for (column_name, de) in column_de {
            println!("{column_name}");
            props =
                props.set_column_dictionary_enabled(ColumnPath::from(column_name), de);
        }
    }

    if let Some(compression_algo) = merge_config.compression {
        if compression_algo.to_lowercase() == "brotli" {
            props = props.set_compression(Compression::BROTLI(
                BrotliLevel::try_new(
                    merge_config
                        .compression_level
                        .expect("Compression level was not set!"),
                )
                .expect("Invalid Brotli level!"),
            ))
        } else if compression_algo.to_lowercase() == "gzip" {
            props = props.set_compression(Compression::GZIP(
                GzipLevel::try_new(
                    merge_config
                        .compression_level
                        .expect("Compression level was not set!"),
                )
                .expect("Invalid GZIP level!"),
            ))
        } else if compression_algo.to_lowercase() == "zstd" {
            props = props.set_compression(Compression::ZSTD(
                ZstdLevel::try_new(
                    merge_config
                        .compression_level
                        .expect("Compression level was not set!")
                        as i32,
                )
                .expect("Invalid ZSTD level!"),
            ))
        }
    }

    Ok(props.build())
}

pub(crate) fn execute(opts: MergeCommandArgs) -> Result<(), PQRSError> {
    debug!("The file names to read are: {:?}", opts.input);
    debug!("The file name to write to: {}", opts.output.display());

    let merge_config = if opts.config.is_some() {
        Some(build_props_from_json_config(opts.config.unwrap())?)
    } else {
        None
    };

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
    write_parquet(combined, &opts.output, merge_config)?;

    Ok(())
}
