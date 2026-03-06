# API Reference

This document provides technical details for the available API endpoints.

## Base URL
All endpoints are relative to the server host (e.g., `http://localhost:8080`).

## Authentication & Headers
The system uses the `X-Consumer-ID` header to identify and track the progress of different clients.

- **`X-Consumer-ID` (Required):** A unique string identifying the consumer (e.g., `partner-service-1`).

---

## 1. Health Check
Checks if the system is up and running.

- **Method:** `GET`
- **Path:** `/health`
- **Response Code:** `200 OK`
- **Example Response:**
  ```json
  {
    "status": "ok",
    "timestamp": "2024-03-07T10:00:00Z"
  }
  ```

## 2. Get Watermark Info
Returns the current progress (bookmark) for a specific consumer.

- **Method:** `GET`
- **Path:** `/exports/watermark`
- **Headers:** `X-Consumer-ID`
- **Response Code:** `200 OK` or `404 Not Found`
- **Example Response:**
  ```json
  {
    "consumerId": "partner-service-1",
    "lastExportedAt": "2024-03-07T09:00:00Z"
  }
  ```

## 3. Trigger Full Export
Exports all records that are not soft-deleted.

- **Method:** `POST`
- **Path:** `/exports/full`
- **Headers:** `X-Consumer-ID`
- **Response Code:** `202 Accepted`
- **Example Request:**
  ```bash
  curl -X POST http://localhost:8080/exports/full -H "X-Consumer-ID: partner-service-1"
  ```
- **Example Response:**
  ```json
  {
    "jobId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "status": "started",
    "exportType": "full",
    "outputFilename": "full_partner-service-1_1709798400000.csv"
  }
  ```

## 4. Trigger Incremental Export
Exports new or updated records (excluding deleted ones) since the last watermark.

- **Method:** `POST`
- **Path:** `/exports/incremental`
- **Headers:** `X-Consumer-ID`
- **Response Code:** `202 Accepted`

## 5. Trigger Delta Export
Exports ALL changes (Insert, Update, and Delete) since the last watermark. Uses the `is_deleted` flag to identify deletions.

- **Method:** `POST`
- **Path:** `/exports/delta`
- **Headers:** `X-Consumer-ID`
- **Response Code:** `202 Accepted`
- **Example Response:**
  ```json
  {
    "jobId": "a1b2c3d4-e5f6-7890-g1h2-i3j4k5l6m7n8",
    "status": "started",
    "exportType": "delta",
    "outputFilename": "delta_partner-service-1_1709798400000.csv"
  }
  ```
