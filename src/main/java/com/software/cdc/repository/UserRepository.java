package com.software.cdc.repository;

import com.software.cdc.models.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.ZonedDateTime;
import java.util.List;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    // For Full Export: Get everyone who isn't soft-deleted
    List<User> findByIsDeletedFalseOrderByUpdatedAtAsc();

    // For Incremental Export: Get new/updated records (excluding deleted) since the
    // watermark
    List<User> findByUpdatedAtGreaterThanAndIsDeletedFalseOrderByUpdatedAtAsc(ZonedDateTime watermark);

    // For Delta Export: Get ALL changes (including deletes) since the watermark
    List<User> findByUpdatedAtGreaterThanOrderByUpdatedAtAsc(ZonedDateTime watermark);
}