package com.software.cdc.repository;

import com.software.cdc.models.Watermark;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface WatermarkRepository extends JpaRepository<Watermark, Long> {

    // Find a specific consumer's progress
    Optional<Watermark> findByConsumerId(String consumerId);
}