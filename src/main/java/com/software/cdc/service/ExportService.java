package com.software.cdc.service;

import com.software.cdc.models.User;
import com.software.cdc.models.Watermark;
import com.software.cdc.repository.UserRepository;
import com.software.cdc.repository.WatermarkRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
public class ExportService {

    private final UserRepository userRepository;
    private final WatermarkRepository watermarkRepository;
    private final ExportService self; // The AOP Proxy self-reference

    public ExportService(UserRepository userRepository, WatermarkRepository watermarkRepository,
            @Lazy ExportService self) {
        this.userRepository = userRepository;
        this.watermarkRepository = watermarkRepository;
        this.self = self;
    }

    // --------------------------------------------------------
    // NEW SYNC METHODS (Shifted from Controller)
    // --------------------------------------------------------

    public Map<String, String> getWatermarkInfo(String consumerId) {
        return watermarkRepository.findByConsumerId(consumerId).map(watermark -> {
            Map<String, String> response = new HashMap<>();
            response.put("consumerId", consumerId);
            response.put("lastExportedAt", watermark.getLastExportedAt().toString());
            return response;
        }).orElse(null); // Return null so the controller knows to send a 404
    }

    public Map<String, String> initiateExport(String consumerId, String exportType) {
        String jobId = UUID.randomUUID().toString();
        String filename = String.format("%s_%s_%d.csv", exportType, consumerId, Instant.now().toEpochMilli());

        // Call the async method via the "self" proxy so it actually runs in the
        // background
        self.processExportAsync(jobId, consumerId, exportType, filename);

        // Build the metadata response for the user
        Map<String, String> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("status", "started");
        response.put("exportType", exportType);
        response.put("outputFilename", filename);

        return response;
    }

    // --------------------------------------------------------
    // ASYNC WORKER & HELPERS
    // --------------------------------------------------------

    @Async
    @Transactional
    public void processExportAsync(String jobId, String consumerId, String exportType, String filename) {
        long startTime = System.currentTimeMillis();
        log.info("Export job started: jobId={}, consumerId={}, exportType={}", jobId, consumerId, exportType);

        try {
            List<User> recordsToExport;
            ZonedDateTime currentWatermark = getConsumerWatermark(consumerId);

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

            Path outputPath = Paths.get("output", filename);
            writeToCsv(recordsToExport, outputPath, exportType);

            if (!recordsToExport.isEmpty()) {
                ZonedDateTime maxUpdatedAt = recordsToExport.get(recordsToExport.size() - 1).getUpdatedAt();
                updateWatermark(consumerId, maxUpdatedAt);
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("Export job completed: jobId={}, rowsExported={}, duration={}ms", jobId, recordsToExport.size(),
                    duration);

        } catch (Exception e) {
            log.error("Export job failed: jobId={}, error={}", jobId, e.getMessage(), e);
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
        if (data.contains(",")) {
            return "\"" + data + "\"";
        }
        return data;
    }
}