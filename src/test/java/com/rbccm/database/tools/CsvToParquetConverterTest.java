package com.rbccm.database.tools;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CsvToParquetConverterTest {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    private MessageType schema;
    private CsvToParquetConverter converter;
    private List<String> csvFilePaths;

    @BeforeEach
    void setUp(@TempDir File tempDir) throws IOException {
        schema = SchemaLoader.loadSchema("schema.json");
        converter = new CsvToParquetConverter(schema, 128 * 1024 * 1024, 2);

        csvFilePaths = Arrays.asList(
                new File(tempDir, "test1.csv").getAbsolutePath(),
                new File(tempDir, "test2.csv").getAbsolutePath()
        );
        for (String csvPath : csvFilePaths) {
            try (FileWriter writer = new FileWriter(csvPath)) {
                writer.write("id,name,age,amount,birth_date,description,large_count,transaction_date,flag,code,account_id,big_number,huge_number,currency_code,event_timestamp,massive_count,quantity,notes,address,email,phone,order_id,status,city,balance,total,comments,uuid\n");
                writer.write("1,Alice,25,1234.56,1990-01-01,Item A,1000000,2023-01-01,1,CODE1,1234567890,9876543210,1234567890,USD,2023-01-01 12:00:00.123456789,1122334455,100,Note here,123 Main St,alice@example.com,123-456-7890,5001,ACTIVE,New York,500000,987654.32,Long comment,uuid-1234\n");
                writer.write("2,Bob,30,5678.90,1985-06-15,Item B,2000000,2023-02-01,0,CODE2,9876543210,1234567890,9876543210,EUR,2023-02-01 13:00:00.987654321,2233445566,200,Another note,456 Elm St,bob@example.com,987-654-3210,5002,INACTIVE,London,1000000,123456.78,Another comment,uuid-5678\n");
            }
        }
    }

    @Test
    void testConvertCsvToParquet(@TempDir File tempDir) throws Exception {
        String outputDir = new File(tempDir, "output").getAbsolutePath();
        converter.convertCsvToParquet(csvFilePaths, outputDir);

        for (String csvPath : csvFilePaths) {
            String parquetFileName = java.nio.file.Path.of(csvPath).getFileName().toString().replace(".csv", ".parquet");
            String parquetFilePath = java.nio.file.Path.of(outputDir, parquetFileName).toString();
            File parquetFile = new File(parquetFilePath);
            assertTrue(parquetFile.exists(), "Parquet file should be created: " + parquetFilePath);

            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(new Path(parquetFilePath), new Configuration())).build()) {
                GenericRecord record = reader.read();
                assertNotNull(record);
                // Verify key fields
                assertEquals(1, record.get("id"));
                assertEquals("Alice", record.get("name").toString());
                assertEquals(25, record.get("age"));
                ByteBuffer amountBuffer = (ByteBuffer) record.get("amount");
                BigDecimal amount = amountBuffer != null ? new BigDecimal(new java.math.BigInteger(amountBuffer.array()), 2) : null;
                assertEquals(new BigDecimal("1234.56").setScale(2, BigDecimal.ROUND_HALF_UP), amount);
                assertEquals((int) LocalDate.parse("1990-01-01", DATE_FORMATTER).toEpochDay(), record.get("birth_date"));
                assertEquals("Item A", record.get("description").toString());
                assertEquals(1000000L, record.get("large_count"));
                assertEquals((int) LocalDate.parse("2023-01-01", DATE_FORMATTER).toEpochDay(), record.get("transaction_date"));
                assertEquals(1, record.get("flag"));
                assertEquals("CODE1", record.get("code").toString());
                assertEquals(1234567890L, record.get("account_id"));
                assertEquals("USD", record.get("currency_code").toString());
                assertEquals(LocalDateTime.parse("2023-01-01 12:00:00.123456789", TIMESTAMP_FORMATTER)
                        .toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000 + 123456, record.get("event_timestamp"));
                assertEquals(100, record.get("quantity"));
                assertEquals("alice@example.com", record.get("email").toString());
                assertEquals("New York", record.get("city").toString());
                ByteBuffer totalBuffer = (ByteBuffer) record.get("total");
                BigDecimal total = totalBuffer != null ? new BigDecimal(new java.math.BigInteger(totalBuffer.array()), 2) : null;
                assertEquals(new BigDecimal("987654.32").setScale(2, BigDecimal.ROUND_HALF_UP), total);
            }
        }
    }
}