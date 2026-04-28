-- stg_consultations_fixed.sql

WITH source AS (
    SELECT * FROM {{ source('raw_teleclinic', 'consultations') }}
),

filtered_source AS (
    -- Filter out test/seed accounts where patient_id begins with 'TEST_'
    SELECT *
    FROM source
    WHERE patient_id NOT LIKE 'TEST_%'
),

timezone_corrected AS (
    SELECT 
        consultation_id,
        patient_id,
        provider_id,
        consultation_created_at,
        
        -- Detects records where consultation_started_at contains a UTC+2 offset 
        -- and normalises them to UTC using ClickHouse's toTimeZone()
        CASE 
            WHEN endsWith(consultation_started_at, 'UTC+2') 
                THEN toTimeZone(parseDateTimeBestEffort(consultation_started_at), 'UTC')
            ELSE parseDateTimeBestEffort(consultation_started_at)
        END AS consultation_started_at_utc,
        
        -- Adds a boolean column, is_tz_corrected to flag which records were adjusted
        CASE 
            WHEN endsWith(consultation_started_at, 'UTC+2') THEN true 
            ELSE false 
        END AS is_tz_corrected
        
    FROM filtered_source
),

calculated_wait_times AS (
    SELECT 
        *,
        -- Calculate wait time in minutes
        dateDiff('minute', consultation_created_at, consultation_started_at_utc) AS raw_wait_time_minutes
    FROM timezone_corrected
)

SELECT 
    consultation_id,
    patient_id,
    provider_id,
    consultation_created_at,
    consultation_started_at_utc,
    is_tz_corrected,
    
    -- Produces a clean wait_time_minutes column, setting NULL where the value remains 
    -- negative or implausibly large after correction.
    -- Threshold: wait times should be >= 0 minutes and < 1440 minutes (24 hours).
    -- Anything outside this range is considered implausible for a teleclinic consultation.
    CASE 
        WHEN raw_wait_time_minutes < 0 THEN NULL
        WHEN raw_wait_time_minutes > 1440 THEN NULL
        ELSE raw_wait_time_minutes
    END AS wait_time_minutes

FROM calculated_wait_times

/*
version: 2
sources:
  - name: raw_teleclinic
    description: "Raw data from the Irembo TeleClinic platform"
    tables:
      - name: consultations
        description: "Raw consultation records including timestamps and patient IDs"
*/

