package com.software.cdc.controller;

import com.software.cdc.models.Watermark;
import com.software.cdc.repository.WatermarkRepository;
import com.software.cdc.service.ExportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@RestController
public class ExportController {

    private final WatermarkRepository watermarkRepository;
    private final ExportService exportService;

    public ExportController(WatermarkRepository watermarkRepository, ExportService exportService) {
        this.watermarkRepository = watermarkRepository;
        this.exportService = exportService;
    }

    // Core Requirement 4: Health Check
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "ok");
        response.put("timestamp", Instant.now().toString());
        return ResponseEntity.ok(response);
    }

    // Core Requirement 8: Get Watermark
    @GetMapping("/exports/watermark")
    public ResponseEntity<?> getWatermark(@RequestHeader("X-Consumer-ID") String consumerId) {
        Optional<Watermark> watermark = watermarkRepository.findByConsumerId(consumerId);

        if (watermark.isPresent()) {
            Map<String, String> response = new HashMap<>();
            response.put("consumerId", consumerId);
            response.put("lastExportedAt", watermark.get().getLastExportedAt().toString());
            return ResponseEntity.ok(response);
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Watermark not found for consumer: " + consumerId);
        }
    }

    // Core Requirement 5: Full Export
    @PostMapping("/exports/full")
    public ResponseEntity<Map<String, String>> triggerFullExport(@RequestHeader("X-Consumer-ID") String consumerId) {
        return handleExportRequest(consumerId, "full");
    }

    // Core Requirement 6: Incremental Export
    @PostMapping("/exports/incremental")
    public ResponseEntity<Map<String, String>> triggerIncrementalExport(
            @RequestHeader("X-Consumer-ID") String consumerId) {
        return handleExportRequest(consumerId, "incremental");
    }

    // Core Requirement 7: Delta Export
    @PostMapping("/exports/delta")
    public ResponseEntity<Map<String, String>> triggerDeltaExport(@RequestHeader("X-Consumer-ID") String consumerId) {
        return handleExportRequest(consumerId, "delta");
    }

    // Helper method to keep controllers clean
    private ResponseEntity<Map<String, String>> handleExportRequest(String consumerId, String exportType) {
        String jobId = UUID.randomUUID().toString();
        String filename = String.format("%s_%s_%d.csv", exportType, consumerId, Instant.now().toEpochMilli());

        // We will uncomment this once we write the Service!
        exportService.processExportAsync(jobId, consumerId, exportType, filename);

        Map<String, String> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("status", "started");
        response.put("exportType", exportType);
        response.put("outputFilename", filename);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}