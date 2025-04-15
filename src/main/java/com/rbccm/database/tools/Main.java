package com.rbccm.database.tools;

import org.apache.parquet.schema.MessageType;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        // Load schema
        MessageType schema = SchemaLoader.loadSchema("schema.json");

        // Convert multiple CSV files to Parquet
        List<String> csvFiles = Arrays.asList(
                "src/main/resources/sample.csv",
                "src/main/resources/sample.csv"
        );
        CsvToParquetConverter converter = new CsvToParquetConverter(schema, 128 * 1024 * 1024, 4);
        String outputDir = "output/parquet";
        converter.convertCsvToParquet(csvFiles, outputDir);

        // Upload Parquet files to S3
        S3Uploader uploader = new S3Uploader(4);
        List<String> parquetFiles = Arrays.asList(
                "output/parquet/sample.parquet",
                "output/parquet/sample.parquet"
        );
        uploader.uploadToS3("your-bucket-name", "data/parquet", parquetFiles);
    }
}