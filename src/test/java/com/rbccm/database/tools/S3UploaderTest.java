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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class S3UploaderTest {
    private S3Client mockS3Client;
    private S3Uploader uploader;

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
        uploader = new S3Uploader(mockS3Client, 2);
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