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
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CsvToParquetConverterTest {
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
                writer.write("id,name,age\n1,Alice,25\n2,Bob,30\n");
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
                assertEquals(1, record.get("id"));
                assertEquals("Alice", record.get("name").toString());
                assertEquals(25, record.get("age"));
            }
        }
    }
}