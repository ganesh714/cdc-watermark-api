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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ExportAsyncWorkerTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private WatermarkRepository watermarkRepository;

    private ExportAsyncWorker exportAsyncWorker;

    private String testConsumerId = "test_consumer";

    @BeforeEach
    public void setup() throws IOException {
        exportAsyncWorker = new ExportAsyncWorker(userRepository, watermarkRepository);
        // Create the /app/output directory for testing if on a system that allows it,
        // otherwise it will use the current directory's app/output
        Files.createDirectories(Paths.get("/app/output"));
    }

    @AfterEach
    public void cleanup() throws IOException {
        Path outputDir = Paths.get("/app/output");
        if (Files.exists(outputDir)) {
            Files.list(outputDir)
                    .filter(Files::isRegularFile)
                    .forEach(f -> {
                        try {
                            Files.delete(f);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }
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
        String filename = "test_full_worker.csv";

        exportAsyncWorker.processExportAsync(jobId, testConsumerId, "full", filename);

        Path out = Paths.get("/app/output", filename);
        assertTrue(Files.exists(out));
        List<String> lines = Files.readAllLines(out);
        assertEquals(2, lines.size()); // header + 1 row

        verify(watermarkRepository, times(1)).save(any(Watermark.class));
    }
}
