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

    private final WatermarkRepository watermarkRepository;
    private final ExportAsyncWorker exportAsyncWorker;

    public ExportService(WatermarkRepository watermarkRepository, ExportAsyncWorker exportAsyncWorker) {
        this.watermarkRepository = watermarkRepository;
        this.exportAsyncWorker = exportAsyncWorker;
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

        // Call the async worker
        exportAsyncWorker.processExportAsync(jobId, consumerId, exportType, filename);

        // Build the metadata response for the user
        Map<String, String> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("status", "started");
        response.put("exportType", exportType);
        response.put("outputFilename", filename);

        return response;
    }

}