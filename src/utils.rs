use crate::errors::PQRSError;
use crate::errors::PQRSError::CouldNotOpenFile;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use log::debug;
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::File;
use std::ops::Add;
use std::path::Path;
use std::sync::Arc;

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

pub fn print_rows(
    file: File,
    num_records: Option<i64>,
    json: bool,
) -> Result<(), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let mut iter = parquet_reader.get_row_iter(None)?;

    let mut start: i64 = 0;
    let end: i64 = num_records.unwrap_or(0);
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

pub fn print_rows_random(
    file: File,
    sample_size: i64,
    json: bool,
) -> Result<(), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file.try_clone()?)?;
    let mut iter = parquet_reader.get_row_iter(None)?;

    let total_records_in_file: i64 = get_row_count(file)?;
    let mut indexes = (0..total_records_in_file).collect::<Vec<_>>();
    // debug!("Original indexes: {:?}", indexes);
    let mut rng = thread_rng();
    indexes.shuffle(&mut rng);
    // debug!("Shuffled indexes: {:?}", indexes);
    indexes = indexes
        .into_iter()
        .take(sample_size as usize)
        .collect::<Vec<_>>();

    debug!("Sampled indexes: {:?}", indexes);

    let mut start: i64 = 0;
    while let Some(row) = iter.next() {
        if indexes.contains(&start) {
            print_row(&row, json)
        }
        start += 1;
    }

    Ok(())
}

#[derive(Debug)]
pub struct ParquetData {
    pub schema: Schema,
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
}

impl Add for ParquetData {
    type Output = Self;

    fn add(mut self, mut rhs: Self) -> Self::Output {
        let mut combined_data = Vec::new();
        combined_data.append(&mut self.batches);
        combined_data.append(&mut rhs.batches);

        Self {
            schema: self.schema,
            batches: combined_data,
            rows: self.rows + rhs.rows,
        }
    }
}

pub fn combine_contents(input: &str) -> Result<ParquetData, PQRSError> {
    let file = open_file(input)?;
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let schema = arrow_reader.get_schema()?;
    let record_batch_reader = arrow_reader.get_record_reader(1024)?;
    let mut batches: Vec<RecordBatch> = Vec::new();

    let mut rows = 0;
    for maybe_batch in record_batch_reader {
        let record_batch = maybe_batch?;
        rows += record_batch.num_rows();

        batches.push(record_batch);
    }

    Ok(ParquetData {
        schema,
        batches,
        rows,
    })
}

pub fn write_parquet(data: ParquetData, output: &str) -> Result<(), PQRSError> {
    let file = File::create(output)?;
    let fields = data.schema.fields().to_vec();
    let schema_without_metadata = Schema::new(fields);

    let mut writer = ArrowWriter::try_new(file, Arc::new(schema_without_metadata), None)?;

    for record_batch in data.batches.iter() {
        writer.write(&record_batch)?;
    }

    writer.close()?;
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
