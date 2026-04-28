# IREMBO-TeleClinic Assignement

This repository contains the data modeling and diagnostic fix for the Irembo TeleClinic platform. The goal of this project was to resolve two major reporting anomalies in the April dashboard: an artificial spike in referral rates and a timezone-related bug affecting patient wait times.

## Project Overview

Following a platform update on April 3rd and a mobile app release on April 5th, two primary issues were identified:
1.  **Metric Inflation:** The monthly referral rate jumped from 11% to 28%.
2.  **Data Integrity/quality:** Average wait times dropped from 38 minutes to 4 minutes due to negative time calculations.

This project implements a robust dbt-based transformation layer to normalize these signals and provide the Ministry of Health with accurate, comparable clinical metrics.

## Data Architecture & Assumptions
### Data Architecture
The solution is built using a three-layer dimensional modeling approach:

*   **Staging (`models/staging`):** fixes timezone bug and cleans wait time. It detects UTC+2 offsets from the new mobile app and normalizes them to UTC. It also filters out test accounts (prefixed with `TEST_`) and nullifies anomalous/inconsistent wait times (e.g., negative values or those >24 hours).
*   **Intermediate (`models/intermediate`):** Classifies every consultation into one of four categories: `doctor_referral`, `patient_requested_only`, `both`, or `no_referral`. This ensures we can distinguish between clinical decisions and patient requests.
*   **Marts (`models/marts`):** It separates the **Clinical Referral Rate** (doctor-issued) from the **Patient Interest Rate** (patient-requested), ensuring that February through April remain perfectly comparable for stakeholders.

### Key Assumptions
-   **Source Schema:** `consultation_started_at` may contain mixed timezone formats (UTC and UTC+2 string offsets)..
-   **Clinical Validity:** A "true" clinical referral is defined as any case where a doctor issued a referral, regardless of whether the patient requested it beforehand.
-   **Wait Time limit/Threshold:** Any wait time exceeding 1,440 minutes (24 hours) is considered a data outlier or a system error and is set to `NULL` to avoid its impact on the average.
- referral_issued and referral_requested are boolean (0/1).
- one row per consultation_id in staging models to avoid double count.

## Data Quality & Testing

To prevent future errors in reporting, we can apply the following testing strategy:
-   **Integrity Tests:** Standard `unique` and `not_null` constraints on primary keys.
-   **Wait Time Assertion:** A custom test in `stg_consultations_fixed` ensures no negative wait times are passed to the reporting layer.
-   **Anomaly Detection:** A custom singular test (`assert_referral_rate_spike_month_over_month.sql`) that fails if the clinical referral rate shifts by more than 5 percentage points month-over-month. This would have caught the April anomaly before it reached the dashboard.

### Folder Structure
#### models/
  ##### staging/        Data cleaning & Time Zone normalization
  ##### intermediate/   Referral classification
  ##### marts/          Final reporting-ready metrics
#### tests/              Custom anomaly detection & data quality

## AI Tool Usage

AI tools were used to assist in structuring dbt models and improve SQL clarity

