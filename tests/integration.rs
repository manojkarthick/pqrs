static SIMPLE_PARQUET_PATH: &'static str = "data/simple.parquet";
static CITIES_PARQUET_PATH: &'static str = "data/cities.parquet";
static PEMS_1_PARQUET_PATH: &'static str = "data/pems-1.snappy.parquet";
static PEMS_2_PARQUET_PATH: &'static str = "data/pems-2.snappy.parquet";
static MERGED_FILE_NAME: &'static str = "merged.snappy.parquet";
static CAT_OUTPUT: &'static str = r#"{continent: "Europe", country: {name: "France", city: ["Paris", "Nice", "Marseilles", "Cannes"]}}
{continent: "Europe", country: {name: "Greece", city: ["Athens", "Piraeus", "Hania", "Heraklion", "Rethymnon", "Fira"]}}
{continent: "North America", country: {name: "Canada", city: ["Toronto", "Vancouver", "St. John's", "Saint John", "Montreal", "Halifax", "Winnipeg", "Calgary", "Saskatoon", "Ottawa", "Yellowknife"]}}
"#;
static CAT_JSON_OUTPUT: &'static str = r#"{"continent":"Europe","country":{"name":"France","city":["Paris","Nice","Marseilles","Cannes"]}}
{"continent":"Europe","country":{"name":"Greece","city":["Athens","Piraeus","Hania","Heraklion","Rethymnon","Fira"]}}
{"continent":"North America","country":{"name":"Canada","city":["Toronto","Vancouver","St. John's","Saint John","Montreal","Halifax","Winnipeg","Calgary","Saskatoon","Ottawa","Yellowknife"]}}
"#;
static CAT_CSV_OUTPUT: &'static str = r#"foo,bar
1,2
10,20"#;
static SCHEMA_OUTPUT: &'static str = r#"message hive_schema {
  OPTIONAL BYTE_ARRAY continent (UTF8);
  OPTIONAL group country {
    OPTIONAL BYTE_ARRAY name (UTF8);
    OPTIONAL group city (LIST) {
      REPEATED group bag {
        OPTIONAL BYTE_ARRAY array_element (UTF8);
      }
    }
  }
}"#;
static SAMPLE_PARTIAL_OUTPUT_1: &'static str = "{continent:";
static SAMPLE_PARTIAL_OUTPUT_2: &'static str = "country: {name:";

/// Integration tests for the crate
mod integration {
    // make sure any new commands added have a corresponding integration test here!
    use crate::{
        CAT_JSON_OUTPUT, CAT_OUTPUT, CITIES_PARQUET_PATH, MERGED_FILE_NAME,
        PEMS_1_PARQUET_PATH, PEMS_2_PARQUET_PATH, SAMPLE_PARTIAL_OUTPUT_1,
        SAMPLE_PARTIAL_OUTPUT_2, SCHEMA_OUTPUT, SIMPLE_PARQUET_PATH, CAT_CSV_OUTPUT,
    };
    use assert_cmd::Command;
    use predicates::prelude::*;
    use tempfile::tempdir;

    #[test]
    fn validate_cat() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("cat").arg(CITIES_PARQUET_PATH);
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(CAT_OUTPUT));

        Ok(())
    }

    #[test]
    fn validate_cat_json() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("cat").arg(CITIES_PARQUET_PATH).arg("--json");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(CAT_JSON_OUTPUT));

        Ok(())
    }

    #[test]
    fn validate_cat_csv() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("cat").arg(SIMPLE_PARQUET_PATH).arg("--csv");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(CAT_CSV_OUTPUT));

        Ok(())
    }

    #[test]
    fn validate_cat_directory() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("cat").arg("data");
        cmd.assert()
            .success()
            .stderr(predicate::str::contains("cities.parquet").and(
                predicate::str::contains("simple.parquet").and(
                    predicate::str::contains("pems-1.snappy.parquet").and(
                        predicate::str::contains("pems-2.snappy.parquet")
                    )
                )
            ));

        Ok(())
    }

    #[test]
    fn validate_head() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        let lines: Vec<&str> = CAT_OUTPUT.split("\n").collect();
        cmd.arg("head").arg(CITIES_PARQUET_PATH).arg("-n").arg("1");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(lines[0]));

        Ok(())
    }

    #[test]
    fn validate_merge() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        let dir = tempdir()?;
        let file_path = dir.path().join(MERGED_FILE_NAME);
        let file_name = file_path.to_str().unwrap();
        cmd.arg("merge")
            .arg("--input")
            .arg(PEMS_1_PARQUET_PATH)
            .arg(PEMS_2_PARQUET_PATH)
            .arg("--output")
            .arg(file_name);
        cmd.assert().success();

        let mut rowcount_cmd = Command::cargo_bin("pqrs")?;
        rowcount_cmd.arg("rowcount").arg(file_name);
        rowcount_cmd
            .assert()
            .success()
            .stdout(predicate::str::contains("5573 rows"));

        dir.close()?;
        Ok(())
    }

    #[test]
    fn validate_rowcount() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("rowcount").arg(CITIES_PARQUET_PATH);
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("3 rows"));

        Ok(())
    }

    #[test]
    fn validate_sample() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("sample")
            .arg(CITIES_PARQUET_PATH)
            .arg("--records")
            .arg("1");
        cmd.assert().success().stdout(
            predicate::str::contains(SAMPLE_PARTIAL_OUTPUT_2)
                .and(predicate::str::starts_with(SAMPLE_PARTIAL_OUTPUT_1)),
        );

        Ok(())
    }

    #[test]
    fn validate_schema() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("schema").arg(CITIES_PARQUET_PATH);
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(SCHEMA_OUTPUT));

        Ok(())
    }

    #[test]
    fn validate_uncompressed_size() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("size").arg(PEMS_1_PARQUET_PATH);
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Uncompressed Size: 63085"));

        Ok(())
    }

    #[test]
    fn validate_uncompressed_size_pretty() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("size").arg(PEMS_1_PARQUET_PATH).arg("--pretty");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Uncompressed Size: 61 KiB"));

        Ok(())
    }

    #[test]
    fn validate_compressed_size() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("size").arg(PEMS_1_PARQUET_PATH).arg("--compressed");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Compressed Size: 13067"));

        Ok(())
    }

    #[test]
    fn validate_compressed_size_pretty() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("pqrs")?;
        cmd.arg("size")
            .arg(PEMS_1_PARQUET_PATH)
            .arg("--compressed")
            .arg("--pretty");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Compressed Size: 12 KiB"));

        Ok(())
    }
}
