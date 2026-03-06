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
    private UserRepository userRepository;

    @Mock
    private WatermarkRepository watermarkRepository;

    @Mock
    private ExportService selfProxy;

    // Injecting mocks and setting the self proxy manually
    private ExportService exportService;

    private String testConsumerId = "test_consumer";

    @BeforeEach
    public void setup() throws IOException {
        exportService = new ExportService(userRepository, watermarkRepository, selfProxy);
        Files.createDirectories(Paths.get("output"));
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

        verify(selfProxy, times(1)).processExportAsync(
                eq(res.get("jobId")), eq(testConsumerId), eq("full"), eq(res.get("outputFilename")));
    }

    @Test
    public void testProcessExportAsync_Full() throws Exception {
        User u1 = new User();
        u1.setId(1L);
        u1.setName("User 1");
        u1.setEmail("u1@example.com");
        u1.setCreatedAt(ZonedDateTime.now().minusDays(1));
        u1.setUpdatedAt(ZonedDateTime.now());
        u1.setDeleted(false);

        when(watermarkRepository.findByConsumerId(testConsumerId)).thenReturn(Optional.empty());
        when(userRepository.findByIsDeletedFalseOrderByUpdatedAtAsc()).thenReturn(Arrays.asList(u1));

        String jobId = "job1";
        String filename = "test_full.csv";

        ExportService realService = new ExportService(userRepository, watermarkRepository, null);
        realService.processExportAsync(jobId, testConsumerId, "full", filename);

        Path out = Paths.get("output", filename);
        assertTrue(Files.exists(out));
        List<String> lines = Files.readAllLines(out);
        assertEquals(2, lines.size()); // header + 1 row

        verify(watermarkRepository, times(1)).save(any(Watermark.class));
    }

    @Test
    public void testProcessExportAsync_Delta() throws Exception {
        User u1 = new User();
        u1.setId(1L);
        u1.setName("User 1, Jr."); // Test CSV escaping
        u1.setEmail("u1@example.com");
        ZonedDateTime now = ZonedDateTime.now();
        u1.setCreatedAt(now);
        u1.setUpdatedAt(now);
        u1.setDeleted(false);

        User u2 = new User();
        u2.setId(2L);
        u2.setName("User 2");
        u2.setEmail("u2@example.com");
        u2.setCreatedAt(now.minusDays(1));
        u2.setUpdatedAt(now);
        u2.setDeleted(true);

        when(watermarkRepository.findByConsumerId(testConsumerId)).thenReturn(Optional.empty());
        when(userRepository.findByUpdatedAtGreaterThanOrderByUpdatedAtAsc(any())).thenReturn(Arrays.asList(u1, u2));

        String jobId = "job2";
        String filename = "test_delta.csv";

        ExportService realService = new ExportService(userRepository, watermarkRepository, null);
        realService.processExportAsync(jobId, testConsumerId, "delta", filename);

        Path out = Paths.get("output", filename);
        assertTrue(Files.exists(out));
        List<String> lines = Files.readAllLines(out);
        assertEquals(3, lines.size()); // header + 2 rows
        assertTrue(lines.get(1).startsWith("INSERT,"));
        assertTrue(lines.get(2).startsWith("DELETE,"));

        verify(watermarkRepository, times(1)).save(any(Watermark.class));
    }
}
