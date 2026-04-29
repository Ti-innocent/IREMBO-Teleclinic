# IREMBO-TeleClinic Assignment

This repository contains the data modeling and diagnostic fix for the Irembo TeleClinic platform. The goal of this project was to resolve two major reporting anomalies in the April dashboard which include referral rates spike and a timezone-related bug affecting patient wait times.

## Project Overview

Following a platform update on April 3rd and a mobile app release on April 5th, two primary issues were identified:
1.  **Metric Changes:** The monthly referral rate jumped from 11% to 28%.
2.  **Data Integrity/quality:** Average wait times dropped from 38 minutes to 4 minutes due to negative time calculations.

## Data Architecture & Assumptions
### Data Architecture
The solution is built using a three-layer dimensional modeling approach:

*   **Staging (`models/staging`):** fixes timezone bug and cleans wait time. It detects UTC+2 offsets from the new mobile app and normalizes them to UTC. It also filters out test accounts (prefixed with `TEST_`) and nullifies anomalous/inconsistent wait times (e.g., negative values or those >24 hours).
*   **Intermediate (`models/intermediate`):** Classifies every consultation into one of four categories: `doctor_referral`, `patient_requested_only`, `both`, or `no_referral`. This ensures we can distinguish between clinical decisions and patient requests.
*   **Marts (`models/marts`):** It separates the doctor-issued referrals from the patient-requested referral, ensuring that February through April remain perfectly comparable for stakeholders.

### Key Assumptions
-   **Source Schema:** `consultation_started_at` may contain mixed timezone formats (UTC and UTC+2 string offsets)..
-   **Clinical Validity:** A "true" clinical referral is defined as any case where a doctor issued a referral, regardless of whether the patient requested it beforehand.
-   **Wait Time limit/Threshold:** Any wait time exceeding 1,440 minutes (24 hours) is considered a data outlier or a system error and is set to `NULL` to avoid its impact on the average.
- referral_issued and referral_requested are boolean (0/1).
- one row per consultation_id in staging models to avoid double count.
### AI Usage
AI tools were used to assist in structuring dbt models and improve SQL clarity

