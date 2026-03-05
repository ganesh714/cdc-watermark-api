package com.software.cdc.controller;

import com.software.cdc.service.ExportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
public class ExportController {

    private final ExportService exportService;

    public ExportController(ExportService exportService) {
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
        // Ask the service for the data
        Map<String, String> watermarkData = exportService.getWatermarkInfo(consumerId);

        if (watermarkData != null) {
            return ResponseEntity.ok(watermarkData);
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Watermark not found for consumer: " + consumerId);
        }
    }

    // Core Requirement 5: Full Export
    @PostMapping("/exports/full")
    public ResponseEntity<Map<String, String>> triggerFullExport(@RequestHeader("X-Consumer-ID") String consumerId) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(exportService.initiateExport(consumerId, "full"));
    }

    // Core Requirement 6: Incremental Export
    @PostMapping("/exports/incremental")
    public ResponseEntity<Map<String, String>> triggerIncrementalExport(
            @RequestHeader("X-Consumer-ID") String consumerId) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(exportService.initiateExport(consumerId, "incremental"));
    }

    // Core Requirement 7: Delta Export
    @PostMapping("/exports/delta")
    public ResponseEntity<Map<String, String>> triggerDeltaExport(@RequestHeader("X-Consumer-ID") String consumerId) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(exportService.initiateExport(consumerId, "delta"));
    }
}