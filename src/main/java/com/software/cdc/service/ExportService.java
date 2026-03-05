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

@Slf4j // Lombok annotation for automatic logging
@Service
public class ExportService {

    private final UserRepository userRepository;
    private final WatermarkRepository watermarkRepository;

    public ExportService(UserRepository userRepository, WatermarkRepository watermarkRepository) {
        this.userRepository = userRepository;
        this.watermarkRepository = watermarkRepository;
    }

    @Async
    @Transactional
    public void processExportAsync(String jobId, String consumerId, String exportType, String filename) {
        long startTime = System.currentTimeMillis();
        // Core Req 10: Log job started
        log.info("Export job started: jobId={}, consumerId={}, exportType={}", jobId, consumerId, exportType);

        try {
            List<User> recordsToExport;
            ZonedDateTime currentWatermark = getConsumerWatermark(consumerId);

            // 1. Fetch the right data based on the export type
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

            // 2. Write data to CSV
            Path outputPath = Paths.get("output", filename);
            writeToCsv(recordsToExport, outputPath, exportType);

            // 3. Update the watermark transactionally (Core Req 9)
            if (!recordsToExport.isEmpty()) {
                ZonedDateTime maxUpdatedAt = recordsToExport.get(recordsToExport.size() - 1).getUpdatedAt();
                updateWatermark(consumerId, maxUpdatedAt);
            }

            long duration = System.currentTimeMillis() - startTime;
            // Core Req 10: Log job completed
            log.info("Export job completed: jobId={}, rowsExported={}, duration={}ms", jobId, recordsToExport.size(),
                    duration);

        } catch (Exception e) {
            // Core Req 10: Log job failed
            log.error("Export job failed: jobId={}, error={}", jobId, e.getMessage(), e);
        }
    }

    private void writeToCsv(List<User> users, Path path, String exportType) throws IOException {
        boolean isDelta = "delta".equals(exportType);

        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            // Write Header
            if (isDelta) {
                writer.write("operation,id,name,email,created_at,updated_at,is_deleted\n");
            } else {
                writer.write("id,name,email,created_at,updated_at,is_deleted\n");
            }

            // Write Rows
            for (User user : users) {
                StringBuilder row = new StringBuilder();

                // Core Req 7: Determine operation for Delta exports
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
                .orElse(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC"))); // Default to beginning of time if no
                                                                                   // watermark
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

    // Helper to prevent CSV breaking if a user's name has a comma
    private String escapeCsv(String data) {
        if (data.contains(",")) {
            return "\"" + data + "\"";
        }
        return data;
    }
}