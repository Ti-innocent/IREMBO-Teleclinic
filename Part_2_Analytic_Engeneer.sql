-- Task 1 — stg_consultations_fixed.sql (timezone fix)

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT *
    FROM {{ source('raw', 'consultations') }}

),

cleaned AS (

    SELECT
        consultation_id,
        patient_id,
        provider_id,

        consultation_created_at,

        consultation_started_at,

        -- Detect UTC+2 timestamps
        CASE
            WHEN position(consultation_started_at, '+02') > 0
                THEN toTimeZone(parseDateTimeBestEffort(consultation_started_at), 'UTC')
            ELSE parseDateTimeBestEffort(consultation_started_at)
        END AS started_at_utc,

        parseDateTimeBestEffort(consultation_created_at) AS created_at_utc,

        -- Flag corrected records
        CASE
            WHEN position(consultation_started_at, '+02') > 0 THEN 1
            ELSE 0
        END AS is_tz_corrected

    FROM source_data
    WHERE patient_id NOT LIKE 'TEST_%'

),

final AS (

    SELECT
        consultation_id,
        patient_id,
        provider_id,
        created_at_utc,
        started_at_utc,
        is_tz_corrected,

        -- Wait time in minutes
        dateDiff('minute', created_at_utc, started_at_utc) AS raw_wait_time,

        -- Clean wait time
        CASE
            WHEN dateDiff('minute', created_at_utc, started_at_utc) < 0 THEN NULL
            WHEN dateDiff('minute', created_at_utc, started_at_utc) > 240 THEN NULL
            ELSE dateDiff('minute', created_at_utc, started_at_utc)
        END AS wait_time_minutes

    FROM cleaned

)

SELECT * FROM final;

-- YAML
version: 2

sources:
  - name: raw
    description: "Raw ingestion layer from TeleClinic platform"
    tables:
      - name: consultations
        description: "Raw consultation request and timing data"
        columns:
          - name: consultation_id
            description: "Unique consultation identifier"
          - name: consultation_created_at
            description: "Timestamp when consultation was requested"
          - name: consultation_started_at
            description: "Timestamp when consultation started"


-- Task 2 — Referral Models
-- 1. int_referrals_classified.sql

{{ config(materialized='view') }}

WITH consultations AS (

    SELECT *
    FROM {{ ref('stg_consultations_fixed') }}

),

outcomes AS (

    SELECT
        consultation_id,
        referral_issued
    FROM {{ ref('stg_clinical_outcomes') }}

),

intake AS (

    SELECT
        consultation_id,
        referral_requested
    FROM {{ ref('stg_consultation_requests') }}

),

joined AS (

    SELECT
        c.consultation_id,

        COALESCE(o.referral_issued, 0) AS doctor_referral,
        COALESCE(i.referral_requested, 0) AS patient_requested

    FROM consultations c
    LEFT JOIN outcomes o USING (consultation_id)
    LEFT JOIN intake i USING (consultation_id)

),

classified AS (

    SELECT
        consultation_id,

        CASE
            WHEN doctor_referral = 1 AND patient_requested = 1 THEN 'both'
            WHEN doctor_referral = 1 THEN 'doctor_referral'
            WHEN patient_requested = 1 THEN 'patient_requested_only'
            ELSE 'no_referral'
        END AS referral_type,

        doctor_referral,
        patient_requested

    FROM joined

)

SELECT * FROM classified;

-- 2. mart_referral_rate_monthly.sql
{{ config(materialized='table') }}

WITH classified AS (

    SELECT *
    FROM {{ ref('int_referrals_classified') }}

),

consultations AS (

    SELECT
        consultation_id,
        created_at_utc
    FROM {{ ref('stg_consultations_fixed') }}

),

joined AS (

    SELECT
        c.consultation_id,
        toStartOfMonth(created_at_utc) AS month,

        r.doctor_referral,
        r.patient_requested

    FROM consultations c
    LEFT JOIN classified r USING (consultation_id)

),

aggregated AS (

    SELECT
        month,

        COUNT(*) AS total_consultations,

        SUM(doctor_referral) AS doctor_referrals,
        SUM(patient_requested) AS patient_requested_referrals,

        ROUND(SUM(doctor_referral) / COUNT(*), 4) AS doctor_referral_rate,
        ROUND(SUM(patient_requested) / COUNT(*), 4) AS patient_requested_rate

    FROM joined
    GROUP BY month

)

SELECT * FROM aggregated;

"""Metric Definition (include below SQL)
Doctor-issued referrals are treated as the primary clinical metric, ensuring consistency across months.
Patient-requested referrals are reported separately, as they reflect patient intent rather than clinical decision-making.
Cases classified as “both” are included in both counts, since they represent agreement between patient and clinician; excluding them would undercount each dimension.
For comparability, historical reporting should rely on doctor_referral_rate, while patient_requested_rate is introduced as a new supporting metric from April onward."""

-- Task 3 — schema_tests.yml
version: 2

models:

  - name: stg_consultations_fixed
    description: "Cleaned consultations with timezone correction"

    columns:
      - name: consultation_id
        description: "Primary key (one row per consultation)"
        tests:
          - unique
          - not_null

      - name: wait_time_minutes
        description: "Consultation wait time in minutes after timezone correction"
        tests:
          - not_null:
              severity: warn

      - name: is_tz_corrected
        description: "Flag indicating whether timezone correction was applied"
        tests:
          - accepted_values:
              values: [0, 1]

  - name: int_referrals_classified
    description: "Classifies consultations into referral types"

    columns:
      - name: referral_type
        description: "Type of referral classification"
        tests:
          - accepted_values:
              values: ['doctor_referral', 'patient_requested_only', 'both', 'no_referral']

  - name: mart_referral_rate_monthly
    description: "Monthly referral metrics for monitoring trends"

    columns:
      - name: month
        description: "Month of consultation"
        tests:
          - not_null

      - name: doctor_referral_rate
        description: "Rate of doctor-issued referrals"
        tests:
          - not_null

      - name: patient_requested_rate
        description: "Rate of patient-requested referrals"
        tests:
          - not_null

-- Custom Test — anomaly detection
-- tests/test_referral_rate_spike.sql
    
WITH rates AS (

    SELECT
        month,
        doctor_referral_rate,
        LAG(doctor_referral_rate) OVER (ORDER BY month) AS prev_rate
    FROM {{ ref('mart_referral_rate_monthly') }}

)

SELECT *
FROM rates
WHERE prev_rate IS NOT NULL
  AND doctor_referral_rate > prev_rate * 1.5

-- Custom Test — timezone validation
SELECT *
FROM {{ ref('stg_consultations_fixed') }}
WHERE wait_time_minutes < 0
