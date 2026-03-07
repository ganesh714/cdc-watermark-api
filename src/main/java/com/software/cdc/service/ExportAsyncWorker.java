package com.software.cdc.service;

import com.software.cdc.models.User;
import com.software.cdc.models.Watermark;
import com.software.cdc.repository.UserRepository;
import com.software.cdc.repository.WatermarkRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

@Slf4j
@Service
public class ExportAsyncWorker {

    private final UserRepository userRepository;
    private final WatermarkRepository watermarkRepository;

    public ExportAsyncWorker(UserRepository userRepository, WatermarkRepository watermarkRepository) {
        this.userRepository = userRepository;
        this.watermarkRepository = watermarkRepository;
    }

    @Async
    @Transactional
    public void processExportAsync(String jobId, String consumerId, String exportType, String filename) {
        long startTime = System.currentTimeMillis();
        // Use an absolute path inside the container to avoid any relative path ambiguity
        Path outputDir = Paths.get("/app/output");
        Path outputPath = outputDir.resolve(filename);

        log.info(">>> ASYNC WORKER ENTERED: jobId={}, target={}", jobId, outputPath);

        try {
            // Ensure directory exists (redundant but safe)
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }

            List<User> recordsToExport;
            ZonedDateTime currentWatermark = getConsumerWatermark(consumerId);
            log.info("JobId {}: Fetching records since {}", jobId, currentWatermark);

            switch (exportType) {
                case "full":
                    recordsToExport = userRepository.findByIsDeletedFalseOrderByUpdatedAtAsc();
                    break;
                case "incremental":
                    recordsToExport = userRepository
                            .findByUpdatedAtGreaterThanAndIsDeletedFalseOrderByUpdatedAtAsc(currentWatermark);
                    break;
                case "delta":
                    recordsToExport = userRepository.findByUpdatedAtGreaterThanOrderByUpdatedAtAsc(currentWatermark);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown export type: " + exportType);
            }

            log.info("JobId {}: Found {} records to export", jobId, recordsToExport.size());

            writeToCsv(recordsToExport, outputPath, exportType);
            log.info("JobId {}: CSV written to {}", jobId, outputPath);

            if (!recordsToExport.isEmpty()) {
                ZonedDateTime maxUpdatedAt = recordsToExport.get(recordsToExport.size() - 1).getUpdatedAt();
                updateWatermark(consumerId, maxUpdatedAt);
                log.info("JobId {}: Watermark updated to {}", jobId, maxUpdatedAt);
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("Export job COMPLETED: jobId={}, duration={}ms", jobId, duration);

        } catch (Exception e) {
            log.error("CRITICAL: Export job FAILED: jobId={}, error={}", jobId, e.getMessage(), e);
        }
    }

    private void writeToCsv(List<User> users, Path path, String exportType) throws IOException {
        boolean isDelta = "delta".equals(exportType);

        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            if (isDelta) {
                writer.write("operation,id,name,email,created_at,updated_at,is_deleted\n");
            } else {
                writer.write("id,name,email,created_at,updated_at,is_deleted\n");
            }

            for (User user : users) {
                StringBuilder row = new StringBuilder();

                if (isDelta) {
                    String operation = "UPDATE";
                    if (user.isDeleted()) {
                        operation = "DELETE";
                    } else if (user.getCreatedAt().isEqual(user.getUpdatedAt())) {
                        operation = "INSERT";
                    }
                    row.append(operation).append(",");
                }

                row.append(user.getId()).append(",")
                        .append(escapeCsv(user.getName())).append(",")
                        .append(user.getEmail()).append(",")
                        .append(user.getCreatedAt()).append(",")
                        .append(user.getUpdatedAt()).append(",")
                        .append(user.isDeleted()).append("\n");

                writer.write(row.toString());
            }
        }
    }

    private ZonedDateTime getConsumerWatermark(String consumerId) {
        return watermarkRepository.findByConsumerId(consumerId)
                .map(Watermark::getLastExportedAt)
                .orElse(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")));
    }

    private void updateWatermark(String consumerId, ZonedDateTime newWatermarkTime) {
        Watermark watermark = watermarkRepository.findByConsumerId(consumerId)
                .orElseGet(() -> {
                    Watermark newMark = new Watermark();
                    newMark.setConsumerId(consumerId);
                    return newMark;
                });

        watermark.setLastExportedAt(newWatermarkTime);
        watermark.setUpdatedAt(ZonedDateTime.now());
        watermarkRepository.save(watermark);
    }

    private String escapeCsv(String data) {
        if (data != null && data.contains(",")) {
            return "\"" + data + "\"";
        }
        return data == null ? "" : data;
    }
}
