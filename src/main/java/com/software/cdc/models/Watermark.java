package com.software.cdc.models;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.ZonedDateTime;

@Entity
@Table(name = "watermarks")
@Data
@NoArgsConstructor
public class Watermark {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "consumer_id", nullable = false, unique = true)
    private String consumerId;

    @Column(name = "last_exported_at", nullable = false)
    private ZonedDateTime lastExportedAt;

    @Column(name = "updated_at", nullable = false)
    private ZonedDateTime updatedAt;
}