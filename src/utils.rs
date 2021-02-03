use crate::errors::PQRSError;
use crate::errors::PQRSError::CouldNotOpenFile;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use std::fs::File;
use std::path::Path;

static ONE_KI_B: i64 = 1024;
static ONE_MI_B: i64 = ONE_KI_B * 1024;
static ONE_GI_B: i64 = ONE_MI_B * 1024;
static ONE_TI_B: i64 = ONE_GI_B * 1024;
static ONE_PI_B: i64 = ONE_TI_B * 1024;

/// Check if a particular path is present on the filesystem
pub fn check_path_present(file_path: &str) -> bool {
    Path::new(file_path).exists()
}

pub fn open_file(file_name: &str) -> Result<File, PQRSError> {
    let path = Path::new(&file_name);
    let file = match File::open(&path) {
        Err(_) => return Err(CouldNotOpenFile(file_name.to_string())),
        Ok(f) => f,
    };

    Ok(file)
}

pub fn print_rows(file: File, num_records: Option<usize>, json: bool) -> Result<(), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let mut iter = parquet_reader.get_row_iter(None)?;

    let mut start = 0;
    let end = num_records.unwrap_or(0);
    let all_records = num_records.is_none();

    while all_records || start < end {
        match iter.next() {
            Some(row) => print_row(&row, json),
            None => break,
        }
        start += 1;
    }

    Ok(())
}

fn print_row(row: &Row, use_json: bool) {
    if use_json {
        println!("{}", row.to_json_value());
    } else {
        println!("{}", row.to_string());
    }
}

pub fn get_row_count(file: File) -> Result<i64, PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let row_group_metadata = parquet_reader.metadata().row_groups();
    let total_num_rows = row_group_metadata.iter().map(|rg| rg.num_rows()).sum();

    Ok(total_num_rows)
}

pub fn get_size(file: File) -> Result<(i64, i64), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let row_group_metadata = parquet_reader.metadata().row_groups();

    let uncompressed_size = row_group_metadata
        .iter()
        .map(|rg| rg.total_byte_size())
        .sum();
    let compressed_size = row_group_metadata
        .iter()
        .map(|rg| rg.compressed_size())
        .sum();

    Ok((uncompressed_size, compressed_size))
}

pub fn get_pretty_size(bytes: i64) -> String {
    if bytes / ONE_KI_B < 1 {
        return format!("{} Bytes", bytes);
    }

    if bytes / ONE_MI_B < 1 {
        return format!("{:.3} KiB", bytes / ONE_KI_B);
    }

    if bytes / ONE_GI_B < 1 {
        return format!("{:.3} MiB", bytes / ONE_MI_B);
    }

    if bytes / ONE_TI_B < 1 {
        return format!("{:.3} GiB", bytes / ONE_GI_B);
    }

    if bytes / ONE_PI_B < 1 {
        return format!("{:.3} TiB", bytes / ONE_TI_B);
    }

    return format!("{:.3} PiB", bytes / ONE_PI_B);
}
