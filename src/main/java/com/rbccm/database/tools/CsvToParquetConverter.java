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
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CsvToParquetConverter {
    private static final Logger logger = LoggerFactory.getLogger(CsvToParquetConverter.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE; // e.g., 1990-01-01
    private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"), // e.g., 2023-01-01 12:00:00.123456789
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),    // e.g., 2023-01-01 12:00:00.123456
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")        // e.g., 2023-01-01 12:00:00.123
    };
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

        return Schema.createRecord(
                parquetSchema.getName(),
                null,
                "com.rbccm.database.tools",
                false,
                avroFields
        );
    }

    private Schema getAvroType(Type parquetType) {
        if (!parquetType.isPrimitive()) {
            throw new IllegalArgumentException("Non-primitive types not supported: " + parquetType);
        }

        var primitive = parquetType.asPrimitiveType();
        var logicalType = parquetType.getLogicalTypeAnnotation();
        switch (primitive.getPrimitiveTypeName()) {
            case INT32:
                return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
            case INT64:
                return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
            case BINARY:
                if (logicalType != null && logicalType.toString().contains("STRING")) {
                    return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
                } else if (logicalType != null && logicalType.toString().contains("DECIMAL")) {
                    return Schema.createUnion(
                            Schema.create(Schema.Type.NULL),
                            Schema.create(Schema.Type.BYTES)
                    );
                }
                throw new IllegalArgumentException("Unsupported BINARY type: " + parquetType);
            default:
                throw new IllegalArgumentException("Unsupported Parquet type: " + parquetType);
        }
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

    private long parseTimestampToMicros(String value) {
        for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
            try {
                LocalDateTime timestamp = LocalDateTime.parse(value, formatter);
                long micros = timestamp.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000
                        + timestamp.getNano() / 1_000;
                logger.debug("Parsed timestamp '{}' to {} micros", value, micros);
                return micros;
            } catch (DateTimeParseException e) {
                // Try next formatter
            }
        }
        throw new DateTimeParseException("Invalid timestamp format: " + value, value, 0);
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
                for (int i = 0; i < parquetSchema.getFields().size(); i++) {
                    Type field = parquetSchema.getFields().get(i);
                    String fieldName = field.getName();
                    var logicalType = field.getLogicalTypeAnnotation();
                    String value = i < record.length ? record[i] : "";
                    if (value == null || value.trim().isEmpty()) {
                        avroRecord.put(fieldName, null);
                        continue;
                    }
                    try {
                        var primitive = field.asPrimitiveType();
                        logger.debug("Processing field {} with logicalType: {}", fieldName, logicalType);
                        switch (primitive.getPrimitiveTypeName()) {
                            case INT32:
                                if (logicalType != null && logicalType.toString().contains("DATE")) {
                                    LocalDate date = LocalDate.parse(value, DATE_FORMATTER);
                                    avroRecord.put(fieldName, (int) date.toEpochDay());
                                } else {
                                    avroRecord.put(fieldName, Integer.parseInt(value));
                                }
                                break;
                            case INT64:
                                if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                                    long micros = parseTimestampToMicros(value);
                                    avroRecord.put(fieldName, micros);
                                } else {
                                    avroRecord.put(fieldName, Long.parseLong(value));
                                }
                                break;
                            case BINARY:
                                if (logicalType != null && logicalType.toString().contains("STRING")) {
                                    avroRecord.put(fieldName, value);
                                } else if (logicalType != null && logicalType.toString().contains("DECIMAL")) {
                                    try {
                                        BigDecimal bd = new BigDecimal(value).setScale(2, BigDecimal.ROUND_HALF_UP);
                                        logger.debug("Parsed decimal value '{}' to {} for field {}", value, bd, fieldName);
                                        avroRecord.put(fieldName, ByteBuffer.wrap(bd.unscaledValue().toByteArray()));
                                    } catch (NumberFormatException e) {
                                        logger.warn("Invalid decimal value '{}' for field {}, setting to null", value, fieldName);
                                        avroRecord.put(fieldName, null);
                                    }
                                } else {
                                    throw new IllegalArgumentException("Unsupported BINARY type for " + fieldName);
                                }
                                break;
                            default:
                                logger.warn("Unsupported field type {} for {}, treating as string", logicalType, fieldName);
                                avroRecord.put(fieldName, value);
                        }
                    } catch (DateTimeParseException e) {
                        logger.error("Failed to parse timestamp '{}' for field {}: {}", value, fieldName, e.getMessage());
                        throw new IllegalArgumentException("Invalid timestamp format: " + value, e);
                    } catch (NumberFormatException e) {
                        logger.error("Failed to parse numeric value '{}' for field {}: {}", value, fieldName, e.getMessage());
                        throw new IllegalArgumentException("Invalid numeric format: " + value, e);
                    } catch (Exception e) {
                        logger.error("Unexpected error parsing value '{}' for field {}: {}", value, fieldName, e.getMessage());
                        throw e;
                    }
                }
                writer.write(avroRecord);
            }
        }

        Files.move(tempParquetPath, Path.of(parquetFilePath), StandardCopyOption.REPLACE_EXISTING);
        logger.debug("Moved temp file to final destination: {}", parquetFilePath);
    }

    private ParquetWriter<GenericRecord> buildParquetWriter(String outputPath) throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.file.impl.disable.cache", true);
        OutputFile outputFile = new LocalOutputFile(Paths.get(outputPath));
        return AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) rowGroupSize)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(conf)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }
}