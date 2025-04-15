package com.rbccm.database.tools;

import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            // Load schema
//            SchemaLoader schemaLoader = new SchemaLoader();
            MessageType schema = SchemaLoader.loadSchema("schema.json");

            // Convert multiple CSV files to Parquet
            List<String> csvFiles = Arrays.asList(
                    "src/main/resources/sample1.csv",
                    "src/main/resources/sample2.csv"
            );
            CsvToParquetConverter converter = new CsvToParquetConverter(schema, 128 * 1024 * 1024, 4);
            String outputDir = "output/parquet";
            converter.convertCsvToParquet(csvFiles, outputDir);

            // Upload Parquet files to S3
            S3Uploader uploader = new S3Uploader(4);
            List<String> parquetFiles = Arrays.asList(
                    "output/parquet/sample1.parquet",
                    "output/parquet/sample2.parquet"
            );
            uploader.uploadToS3("swcsample-bucket-name", "swcca", parquetFiles);
            uploader.close();

            logger.info("CSV to Parquet conversion and S3 upload completed successfully");
        } catch (IOException | InterruptedException e) {
            logger.error("Application failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}