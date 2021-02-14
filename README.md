# pqrs

* `pqrs` is a command line tool for inspecting [Parquet](https://parquet.apache.org/) files
* This is a replacement for the [parquet-tools](https://github.com/apache/parquet-mr/tree/master/parquet-tools-deprecated) utility written in Rust
* Built using the Rust implementation of [Parquet](https://github.com/apache/arrow/tree/master/rust/parquet) and [Arrow](https://github.com/apache/arrow/tree/master/rust/arrow)
* `pqrs` roughly means "parquet-tools in rust"


## Installation

### Recommended Method

You can download release binaries [here](https://github.com/manojkarthick/pqrs/releases)

### Alternative methods

#### Using Homebrew

For macOS users, `pqrs` is available as a homebrew tap.

```
brew tap manojkarthick/pqrs
brew install pqrs
```

#### Using nix

TODO

#### Building and running from source

Make sure you have `rustc` and `cargo` installed on your machine.

```
git clone https://github.com/manojkarthick/pqrs.git
cargo build --release
./target/release/pqrs
```

## Running

The below snippet shows the available subcommands:

```
❯ pqrs --help
pqrs 0.1.1
Manoj Karthick
Apache Parquet command-line utility

USAGE:
    pqrs [FLAGS] [SUBCOMMAND]

FLAGS:
    -d, --debug      Show debug output
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    cat         Prints the contents of Parquet file(s)
    head        Prints the first n records of the Parquet file
    help        Prints this message or the help of the given subcommand(s)
    merge       Merge file(s) into another parquet file
    rowcount    Prints the count of rows in Parquet file(s)
    sample      Prints a random sample of records from the Parquet file
    schema      Prints the schema of Parquet file(s)
    size        Prints the size of Parquet file(s)
```

### Subcommand: cat

Prints the contents of the parquet file in a json-like or json format. Use `--json` for JSON output.

```
❯ pqrs cat data/cities.parquet
{continent: "Europe", country: {name: "France", city: ["Paris", "Nice", "Marseilles", "Cannes"]}}
{continent: "Europe", country: {name: "Greece", city: ["Athens", "Piraeus", "Hania", "Heraklion", "Rethymnon", "Fira"]}}
{continent: "North America", country: {name: "Canada", city: ["Toronto", "Vancouver", "St. John's", "Saint John", "Montreal", "Halifax", "Winnipeg", "Calgary", "Saskatoon", "Ottawa", "Yellowknife"]}}
```

```
❯ pqrs cat data/cities.parquet --json
{"continent":"Europe","country":{"name":"France","city":["Paris","Nice","Marseilles","Cannes"]}}
{"continent":"Europe","country":{"name":"Greece","city":["Athens","Piraeus","Hania","Heraklion","Rethymnon","Fira"]}}
{"continent":"North America","country":{"name":"Canada","city":["Toronto","Vancouver","St. John's","Saint John","Montreal","Halifax","Winnipeg","Calgary","Saskatoon","Ottawa","Yellowknife"]}}
```

### Subcommand: head

Prints the first N records of the parquet file. Use `--records` flag to set the number of records.

```
❯ pqrs head data/cities.parquet --json --records 2
{"continent":"Europe","country":{"name":"France","city":["Paris","Nice","Marseilles","Cannes"]}}
{"continent":"Europe","country":{"name":"Greece","city":["Athens","Piraeus","Hania","Heraklion","Rethymnon","Fira"]}}
```

### Subcommand: merge

Merge two Parquet files by placing row groups (or blocks) from the two files one after the other.

Disclaimer: This does not combine the files to have optimized row groups, do not use it in production!

```
❯ pqrs merge --input data/pems-1.snappy.parquet data/pems-2.snappy.parquet --output data/pems-merged.snappy.parquet

❯ ls -al data
total 408
drwxr-xr-x   6 manojkarthick  staff     192 Feb 14 08:53 .
drwxr-xr-x  20 manojkarthick  staff     640 Feb 14 08:52 ..
-rw-r--r--   1 manojkarthick  staff     866 Feb  8 19:50 cities.parquet
-rw-r--r--   1 manojkarthick  staff   16468 Feb  8 19:50 pems-1.snappy.parquet
-rw-r--r--   1 manojkarthick  staff   17342 Feb  8 19:50 pems-2.snappy.parquet
-rw-r--r--   1 manojkarthick  staff  160950 Feb 14 08:53 pems-merged.snappy.parquet
```

### Subcommand: rowcount

Print the number of rows present in the parquet file.

```
❯ pqrs rowcount data/pems-1.snappy.parquet data/pems-2.snappy.parquet
File Name: data/pems-1.snappy.parquet: 2693 rows
File Name: data/pems-2.snappy.parquet: 2880 rows
```

### Subcommand: sample

Prints a random sample of records from the given parquet file.

```
❯ pqrs sample data/pems-1.snappy.parquet --records 3
{timeperiod: "01/17/2016 07:01:27", flow1: 0, occupancy1: 0E0, speed1: 0E0, flow2: 0, occupancy2: 0E0, speed2: 0E0, flow3: 0, occupancy3: 0E0, speed3: 0E0, flow4: null, occupancy4: null, speed4: null, flow5: null, occupancy5: null, speed5: null, flow6: null, occupancy6: null, speed6: null, flow7: null, occupancy7: null, speed7: null, flow8: null, occupancy8: null, speed8: null}
{timeperiod: "01/17/2016 07:47:27", flow1: 0, occupancy1: 0E0, speed1: 0E0, flow2: 0, occupancy2: 0E0, speed2: 0E0, flow3: 0, occupancy3: 0E0, speed3: 0E0, flow4: null, occupancy4: null, speed4: null, flow5: null, occupancy5: null, speed5: null, flow6: null, occupancy6: null, speed6: null, flow7: null, occupancy7: null, speed7: null, flow8: null, occupancy8: null, speed8: null}
{timeperiod: "01/17/2016 09:44:27", flow1: 0, occupancy1: 0E0, speed1: 0E0, flow2: 0, occupancy2: 0E0, speed2: 0E0, flow3: 0, occupancy3: 0E0, speed3: 0E0, flow4: null, occupancy4: null, speed4: null, flow5: null, occupancy5: null, speed5: null, flow6: null, occupancy6: null, speed6: null, flow7: null, occupancy7: null, speed7: null, flow8: null, occupancy8: null, speed8: null}
```

### Subcommand: schema

Print the schema from the given parquet file. Use the `--detailed` flag to get more detailed stats.

```
❯ pqrs schema data/cities.parquet
Metadata for file: data/cities.parquet

version: 1
num of rows: 3
created by: parquet-mr version 1.5.0-cdh5.7.0 (build ${buildNumber})
message hive_schema {
  OPTIONAL BYTE_ARRAY continent (UTF8);
  OPTIONAL group country {
    OPTIONAL BYTE_ARRAY name (UTF8);
    OPTIONAL group city (LIST) {
      REPEATED group bag {
        OPTIONAL BYTE_ARRAY array_element (UTF8);
      }
    }
  }
}
```

```
❯ pqrs schema data/cities.parquet --detailed

num of row groups: 1
row groups:

row group 0:
--------------------------------------------------------------------------------
total byte size: 466
num of rows: 3

num of columns: 3
columns:

column 0:
--------------------------------------------------------------------------------
column type: BYTE_ARRAY
column path: "continent"
encodings: BIT_PACKED PLAIN_DICTIONARY RLE
file path: N/A
file offset: 4
num of values: 3
total compressed size (in bytes): 93
total uncompressed size (in bytes): 93
data page offset: 4
index page offset: N/A
dictionary page offset: N/A
statistics: {min: [69, 117, 114, 111, 112, 101], max: [78, 111, 114, 116, 104, 32, 65, 109, 101, 114, 105, 99, 97], distinct_count: N/A, null_count: 0, min_max_deprecated: true}

<....output clipped>

```

### Subcommand: size

Print the compressed/uncompressed size of the parquet file. Shows uncompressed size by default

```
❯ pqrs size data/pems-1.snappy.parquet --pretty
Size in Bytes:

File Name: data/pems-1.snappy.parquet
Uncompressed Size: 61 KiB
```

```
❯ pqrs size data/pems-1.snappy.parquet --pretty --compressed
Size in Bytes:

File Name: data/pems-1.snappy.parquet
Compressed Size: 12 KiB
```



### TODO

* [ ] Add crate
* [ ] Test on Windows
