package com.rbccm.database.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3Uploader {
    private static final Logger logger = LoggerFactory.getLogger(S3Uploader.class);
    private final S3Client s3Client;
    private final int numThreads;

    public S3Uploader(int numThreads) {
//        String accessKey = "swcusename";
//        String secretKey = "d3elwQedbS/daqS4pf+ElQUb4beowqdqqqqfakecode";
//        String endpoint = "https://s3.devfg.samplecorp.com:9021";

        // Load from environment variables
        String accessKey = System.getenv("S3_ACCESS_KEY");
        String secretKey = System.getenv("S3_SECRET_KEY");
        String endpoint = System.getenv("S3_ENDPOINT");

        if (accessKey == null || secretKey == null || endpoint == null) {
            throw new IllegalStateException("Missing S3 configuration in environment variables: S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT");
        }

        this.s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .endpointOverride(URI.create(endpoint))
                .httpClient(ApacheHttpClient.builder().build())
                // Region is a placeholder for custom endpoints
                .region(Region.US_EAST_1)
                .build();
        this.numThreads = numThreads;
    }

    public S3Uploader(S3Client s3Client, int numThreads) {
        this.s3Client = s3Client;
        this.numThreads = numThreads;
    }

    public void uploadToS3(String bucketName, String prefix, List<String> filePaths) throws InterruptedException {
        logger.info("Starting upload of {} files to S3 bucket: {}, prefix: {}", filePaths.size(), bucketName, prefix);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (String filePath : filePaths) {
            executor.submit(() -> {
                try {
                    String key = prefix + "/" + Path.of(filePath).getFileName().toString();
                    uploadSingleFile(bucketName, key, filePath);
                    logger.info("Successfully uploaded {} to s3://{}/{}", filePath, bucketName, key);
                } catch (Exception e) {
                    logger.error("Failed to upload {}: {}", filePath, e.getMessage(), e);
                }
            });
        }

        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
            logger.warn("Upload tasks did not complete within timeout");
            executor.shutdownNow();
        }
        logger.info("All uploads to S3 completed");
    }

    private void uploadSingleFile(String bucketName, String key, String filePath) throws IOException {
        logger.debug("Uploading {} to s3://{}/{}", filePath, bucketName, key);
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromFile(Path.of(filePath)));
        } catch (S3Exception | SdkClientException e) {
            throw new IOException("Failed to upload " + filePath + " to S3", e);
        }
    }

    // Optional: Close S3 client when done
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
}