package com.rbccm.database.tools;

import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        AtomicBoolean hasErrors = new AtomicBoolean(false);
        try {
            // Load configuration
            Properties config = loadConfig();

            // Load schema
            MessageType schema = SchemaLoader.loadSchema("schema.json");

            // Get CSV files from SourceFilePath
            String sourceFilePath = config.getProperty("SourceFilePath");
            if (sourceFilePath == null) {
                throw new IOException("Missing SourceFilePath in application.properties");
            }
            List<String> csvFiles = Files.list(Paths.get(sourceFilePath))
                    .filter(p -> p.toString().endsWith(".csv"))
                    .map(Path::toString)
                    .collect(Collectors.toList());
            if (csvFiles.isEmpty()) {
                throw new IOException("No CSV files found in " + sourceFilePath);
            }
            logger.info("Found CSV files: {}", csvFiles);

            // Convert CSV files to Parquet
            CsvToParquetConverter converter = new CsvToParquetConverter(schema, 128 * 1024 * 1024, 4);
            String outputDir = config.getProperty("ParquetFilePath");
            if (outputDir == null) {
                throw new IOException("Missing ParquetFilePath in application.properties");
            }
            converter.convertCsvToParquet(csvFiles, outputDir);

            // Get Parquet files from ParquetFilePath
            List<String> parquetFiles = Files.list(Paths.get(outputDir))
                    .filter(p -> p.toString().endsWith(".parquet"))
                    .map(Path::toString)
                    .collect(Collectors.toList());
            if (parquetFiles.isEmpty()) {
                throw new IOException("No Parquet files found in " + outputDir);
            }
            logger.info("Found Parquet files: {}", parquetFiles);

            // Upload Parquet files to S3
            try (S3Uploader uploader = new S3Uploader(4, config)) {
                String bucketName = config.getProperty("bucketName");
                String prefix = config.getProperty("prefix");
                if (bucketName == null || prefix == null) {
                    throw new IOException("Missing bucketName or prefix in application.properties");
                }
                uploader.uploadToS3(bucketName, prefix, parquetFiles);
            } catch (Exception e) {
                logger.error("S3 upload failed: {}", e.getMessage(), e);
                hasErrors.set(true);
            }

            if (hasErrors.get()) {
                throw new IOException("One or more S3 uploads failed");
            }

            logger.info("CSV to Parquet conversion and S3 upload completed successfully");
        } catch (IOException | InterruptedException e) {
            logger.error("Application failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static Properties loadConfig() throws IOException {
        Properties config = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IOException("application.properties not found in classpath");
            }
            config.load(input);
        }
        return config;
    }
}