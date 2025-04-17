package com.rbccm.database.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3Uploader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(S3Uploader.class);
    private final S3Client s3Client;
    private final int numThreads;
    private final ExecutorService executor;

    public S3Uploader(int numThreads, Properties config) {
        this.numThreads = numThreads;
        this.executor = Executors.newFixedThreadPool(numThreads);

        String accessKey = config.getProperty("s3.accessKey");
        String secretKey = config.getProperty("s3.secretKey");
        String endpoint = config.getProperty("s3.endpoint");
        String region = config.getProperty("s3.region");

        if (accessKey == null || secretKey == null || endpoint == null || region == null) {
            throw new IllegalStateException("S3 credentials, endpoint, or region not set in application.properties");
        }

        s3Client = S3Client.builder()
                .endpointOverride(java.net.URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .httpClient(ApacheHttpClient.builder().build())
                .build();
    }

    public void uploadToS3(String bucketName, String prefix, List<String> filePaths) throws IOException, InterruptedException {
        logger.info("Starting upload of {} files to S3 bucket: {}/{}", filePaths.size(), bucketName, prefix);
        AtomicBoolean hasErrors = new AtomicBoolean(false);
        for (String filePath : filePaths) {
            executor.submit(() -> {
                try {
                    uploadSingleFile(bucketName, prefix, filePath);
                    logger.info("Successfully uploaded {}", filePath);
                } catch (Exception e) {
                    logger.error("Failed to upload {}: {}", filePath, e.getMessage(), e);
                    hasErrors.set(true);
                }
            });
        }

        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
            logger.warn("Upload tasks did not complete within timeout");
            executor.shutdownNow();
        }

        if (hasErrors.get()) {
            throw new IOException("One or more files failed to upload to S3");
        }

        logger.info("All uploads to S3 completed");
    }

    private void uploadSingleFile(String bucketName, String prefix, String filePath) throws IOException {
        Path path = Path.of(filePath);
        String key = prefix + "/" + path.getFileName();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try {
            s3Client.putObject(request, RequestBody.fromFile(path));
            logger.debug("Uploaded {} to s3://{}/{}", filePath, bucketName, key);
        } catch (Exception e) {
            throw new IOException("Failed to upload " + filePath + " to S3", e);
        }
    }

    @Override
    public void close() {
        try {
            if (!executor.isShutdown()) {
                executor.shutdownNow();
            }
            if (s3Client != null) {
                s3Client.close();
                logger.info("S3 client closed");
            }
        } catch (Exception e) {
            logger.error("Failed to close S3Uploader: {}", e.getMessage(), e);
        }
    }
}