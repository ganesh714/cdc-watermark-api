package com.software.cdc.service;

import com.software.cdc.models.User;
import com.software.cdc.models.Watermark;
import com.software.cdc.repository.UserRepository;
import com.software.cdc.repository.WatermarkRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ExportServiceTest {

    @Mock
    private WatermarkRepository watermarkRepository;

    @Mock
    private ExportAsyncWorker exportAsyncWorker;

    private ExportService exportService;

    private String testConsumerId = "test_consumer";

    @BeforeEach
    public void setup() {
        exportService = new ExportService(watermarkRepository, exportAsyncWorker);
    }

    @AfterEach
    public void cleanup() throws IOException {
        // Clean up output files
        Files.list(Paths.get("output"))
                .filter(Files::isRegularFile)
                .forEach(f -> {
                    try {
                        Files.delete(f);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void testGetWatermarkInfo_Found() {
        Watermark wm = new Watermark();
        wm.setConsumerId(testConsumerId);
        wm.setLastExportedAt(ZonedDateTime.now());

        when(watermarkRepository.findByConsumerId(testConsumerId)).thenReturn(Optional.of(wm));

        Map<String, String> res = exportService.getWatermarkInfo(testConsumerId);
        assertNotNull(res);
        assertEquals(testConsumerId, res.get("consumerId"));
    }

    @Test
    public void testGetWatermarkInfo_NotFound() {
        when(watermarkRepository.findByConsumerId(testConsumerId)).thenReturn(Optional.empty());

        Map<String, String> res = exportService.getWatermarkInfo(testConsumerId);
        assertNull(res);
    }

    @Test
    public void testInitiateExport() {
        Map<String, String> res = exportService.initiateExport(testConsumerId, "full");

        assertNotNull(res.get("jobId"));
        assertEquals("started", res.get("status"));
        assertEquals("full", res.get("exportType"));
        assertTrue(res.get("outputFilename").contains("full_test_consumer_"));

        verify(exportAsyncWorker, times(1)).processExportAsync(
                eq(res.get("jobId")), eq(testConsumerId), eq("full"), eq(res.get("outputFilename")));
    }

}
