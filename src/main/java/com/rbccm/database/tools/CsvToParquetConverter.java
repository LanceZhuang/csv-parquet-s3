package com.rbccm.database.tools;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CsvToParquetConverter {
    private static final Logger logger = LoggerFactory.getLogger(CsvToParquetConverter.class);
    private final MessageType parquetSchema;
    private final Schema avroSchema;
    private final int rowGroupSize;
    private final int numThreads;

    public CsvToParquetConverter(MessageType parquetSchema, int rowGroupSize, int numThreads) {
        this.parquetSchema = parquetSchema;
        this.avroSchema = convertToAvroSchema(parquetSchema);
        this.rowGroupSize = rowGroupSize;
        this.numThreads = numThreads;
    }

    private Schema convertToAvroSchema(MessageType parquetSchema) {
        List<Schema.Field> avroFields = new ArrayList<>();
        for (Type field : parquetSchema.getFields()) {
            String fieldName = field.getName();
            Schema fieldSchema = getAvroType(field);
            avroFields.add(new Schema.Field(fieldName, fieldSchema, null, null));
        }

        Schema recordSchema = Schema.createRecord(
                parquetSchema.getName(),
                null,
                "com.rbccm.database.tools",
                false,
                avroFields
        );
        return recordSchema;
    }

    private Schema getAvroType(Type parquetType) {
        if (parquetType.isPrimitive()) {
            switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32:
                    return Schema.create(Schema.Type.INT);
                case BINARY:
                    if (parquetType.getOriginalType() == OriginalType.UTF8) {
                        return Schema.create(Schema.Type.STRING);
                    }
                    throw new IllegalArgumentException("Unsupported BINARY type without UTF8: " + parquetType);
                default:
                    throw new IllegalArgumentException("Unsupported Parquet type: " + parquetType);
            }
        }
        throw new IllegalArgumentException("Non-primitive types not supported: " + parquetType);
    }

    public void convertCsvToParquet(List<String> csvFilePaths, String outputDir) throws IOException, InterruptedException {
        logger.info("Starting conversion of {} CSV files to Parquet in directory: {}", csvFilePaths.size(), outputDir);
        Files.createDirectories(Path.of(outputDir));

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (String csvFilePath : csvFilePaths) {
            executor.submit(() -> {
                try {
                    String fileName = Path.of(csvFilePath).getFileName().toString().replace(".csv", ".parquet");
                    String parquetFilePath = Path.of(outputDir, fileName).toString();
                    convertSingleCsvToParquet(csvFilePath, parquetFilePath);
                    logger.info("Successfully converted {} to {}", csvFilePath, parquetFilePath);
                } catch (Exception e) {
                    logger.error("Failed to convert {}: {}", csvFilePath, e.getMessage(), e);
                }
            });
        }

        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
            logger.warn("Conversion tasks did not complete within timeout");
            executor.shutdownNow();
        }
        logger.info("All CSV to Parquet conversions completed");
    }

    private void convertSingleCsvToParquet(String csvFilePath, String parquetFilePath) throws IOException, CsvValidationException {
        logger.debug("Converting {} to {}", csvFilePath, parquetFilePath);
        String fileName = Path.of(csvFilePath).getFileName().toString().replace(".csv", "");
        Path tempParquetPath = Files.createTempFile("parquet_" + fileName + "_", ".parquet");
        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath));
             ParquetWriter<GenericRecord> writer = buildParquetWriter(tempParquetPath.toString())) {

            csvReader.skip(1); // Skip header
            String[] record;
            while ((record = csvReader.readNext()) != null) {
                GenericRecord avroRecord = new GenericData.Record(avroSchema);
                avroRecord.put("id", Integer.parseInt(record[0]));
                avroRecord.put("name", record[1]);
                avroRecord.put("age", Integer.parseInt(record[2]));
                writer.write(avroRecord);
            }
        }

        Files.move(tempParquetPath, Path.of(parquetFilePath), StandardCopyOption.REPLACE_EXISTING);
        logger.debug("Moved temp file to final destination: {}", parquetFilePath);
    }

    private ParquetWriter<GenericRecord> buildParquetWriter(String outputPath) throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.file.impl.disable.cache", true);
        // Use LocalOutputFile to avoid Hadoop path issues on Windows
        OutputFile outputFile = new LocalOutputFile(Paths.get(outputPath));
        return AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(conf)
                .withValidation(false)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }
}