package com.rbccm.database.tools;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class S3UploaderTest {
    private S3Client mockS3Client;
    private S3Uploader uploader;
    private Properties mockConfig;

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
        mockConfig = new Properties();

        // Set configuration properties
        mockConfig.setProperty("s3.accessKey", "test-access-key");
        mockConfig.setProperty("s3.secretKey", "test-secret-key");
        mockConfig.setProperty("s3.endpoint", "https://test-endpoint:9000");
        mockConfig.setProperty("s3.region", "test-region");

        // Use reflection to inject mock S3Client
        uploader = new S3Uploader(2, mockConfig) {
            {
                try {
                    java.lang.reflect.Field s3ClientField = S3Uploader.class.getDeclaredField("s3Client");
                    s3ClientField.setAccessible(true);
                    s3ClientField.set(this, mockS3Client);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to inject mock S3Client", e);
                }
            }
        };
    }

    @Test
    void testUploadToS3(@TempDir File tempDir) throws IOException, InterruptedException {
        List<String> filePaths = Arrays.asList(
                new File(tempDir, "test1.parquet").getAbsolutePath(),
                new File(tempDir, "test2.parquet").getAbsolutePath()
        );
        for (String filePath : filePaths) {
            Files.write(java.nio.file.Path.of(filePath), new byte[]{1, 2, 3});
        }

        PutObjectResponse mockResponse = PutObjectResponse.builder().build();
        when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(mockResponse);

        uploader.uploadToS3("test-bucket", "test-prefix", filePaths);

        verify(mockS3Client, times(filePaths.size())).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
}