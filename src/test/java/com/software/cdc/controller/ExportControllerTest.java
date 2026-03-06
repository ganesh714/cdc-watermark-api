package com.software.cdc.controller;

import com.software.cdc.service.ExportService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ExportController.class)
public class ExportControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ExportService exportService;

    @Test
    public void testHealthCheck() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ok"))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    public void testGetWatermark_Found() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("consumerId", "test-consumer");
        data.put("lastExportedAt", "2023-10-01T12:00:00Z");

        when(exportService.getWatermarkInfo("test-consumer")).thenReturn(data);

        mockMvc.perform(get("/exports/watermark")
                .header("X-Consumer-ID", "test-consumer"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consumerId").value("test-consumer"))
                .andExpect(jsonPath("$.lastExportedAt").value("2023-10-01T12:00:00Z"));
    }

    @Test
    public void testGetWatermark_NotFound() throws Exception {
        when(exportService.getWatermarkInfo("unknown-consumer")).thenReturn(null);

        mockMvc.perform(get("/exports/watermark")
                .header("X-Consumer-ID", "unknown-consumer"))
                .andExpect(status().isNotFound())
                .andExpect(content().string("Watermark not found for consumer: unknown-consumer"));
    }

    @Test
    public void testTriggerFullExport() throws Exception {
        Map<String, String> response = new HashMap<>();
        response.put("jobId", "job-123");
        response.put("status", "started");
        response.put("exportType", "full");

        when(exportService.initiateExport("test-consumer", "full")).thenReturn(response);

        mockMvc.perform(post("/exports/full")
                .header("X-Consumer-ID", "test-consumer"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.jobId").value("job-123"))
                .andExpect(jsonPath("$.status").value("started"))
                .andExpect(jsonPath("$.exportType").value("full"));
    }

    @Test
    public void testTriggerIncrementalExport() throws Exception {
        Map<String, String> response = new HashMap<>();
        response.put("jobId", "job-123");
        response.put("status", "started");
        response.put("exportType", "incremental");

        when(exportService.initiateExport("test-consumer", "incremental")).thenReturn(response);

        mockMvc.perform(post("/exports/incremental")
                .header("X-Consumer-ID", "test-consumer"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.jobId").value("job-123"))
                .andExpect(jsonPath("$.status").value("started"))
                .andExpect(jsonPath("$.exportType").value("incremental"));
    }

    @Test
    public void testTriggerDeltaExport() throws Exception {
        Map<String, String> response = new HashMap<>();
        response.put("jobId", "job-123");
        response.put("status", "started");
        response.put("exportType", "delta");

        when(exportService.initiateExport("test-consumer", "delta")).thenReturn(response);

        mockMvc.perform(post("/exports/delta")
                .header("X-Consumer-ID", "test-consumer"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.jobId").value("job-123"))
                .andExpect(jsonPath("$.status").value("started"))
                .andExpect(jsonPath("$.exportType").value("delta"));
    }
}
