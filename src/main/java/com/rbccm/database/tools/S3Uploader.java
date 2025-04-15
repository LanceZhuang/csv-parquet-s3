package com.rbccm.database.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3Uploader {
    private static final Logger logger = LoggerFactory.getLogger(S3Uploader.class);
    private final S3Client s3Client;
    private final int numThreads;

    public S3Uploader(int numThreads) {
        this.s3Client = S3Client.builder().build();
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

    private void uploadSingleFile(String bucketName, String key, String filePath) {
        logger.debug("Uploading {} to s3://{}/{}", filePath, bucketName, key);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3Client.putObject(putObjectRequest, RequestBody.fromFile(Path.of(filePath)));
    }
}