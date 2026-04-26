# IREMBO-TeleClinic
Remote assignement for Analytics Engineer

**Overview**
This repository contains my solution to the Irembo TeleClinic Analytics Engineer assignment. It includes:

- Root cause analysis of anomalies in referral rate and wait time.
- dbt models to clean, transform, and aggregate consultation data.
- Data quality and anomaly detection tests.

## Key Findings (Part 1)

### Referral Rate Spike (from 11% to 28%)
- Caused by a **metric definition change**, not a clinical conditions from doctors.
- April includes both:
  - Doctor-issued referrals ( approximately to 11%)
  - Patient-requested referrals ( approximately to 17%)
- New intake form (April 3) introduced patient-requested referrals before consultation.

### Wait Time Drop (from 38 to 4 minutes)
- Caused by **timezone inconsistency (UTC vs UTC+2)**.
- Negative wait times skewed averages downward.

## Project Structure

- **staging/**
  - `stg_consultations_fixed.sql`: fixes timezone bug and cleans wait time.
- **intermediate/**
  - `int_referrals_classified.sql`: classifies referral types.
- **marts/**
  - `mart_referral_rate_monthly.sql`: produces comparable monthly metrics.
- **tests/**
  - Detects anomalies and validates data quality.
## Key Modeling Decisions

- Standardized all timestamps to UTC.
- Defined valid wait time range: **0–240 minutes**.
- Separated:
  - doctor_referral (clinical signal)
  - patient_requested (user input)
- Included "both" in both categories to avoid undercounting.

## Assumptions:
- consultation_started_at may contain mixed timezone formats (UTC and UTC+2 string offsets).
- referral_issued and referral_requested are boolean (0/1).
- one row per consultation_id in staging models to avoid double count.

## Key decisions:
- Standardized all timestamps to UTC before calculating wait times.
- Defined valid wait time range as 0–240 minutes.
- Separated patient-requested and doctor-issued referrals for metric clarity with 0 and 1.
- Included "both" category in both aggregates to preserve counts.

## Testing Strategy

- Generic tests:
  - `unique`, `not_null`, `accepted_values`
  - Enforced accepted values for referral classification.
- Custom tests:
  - Referral spike detection (>50% Month over Month increase).
  - No negative wait times after timezone correction.

## AI Usage

AI tools were used to:
- Structure dbt models
- Improve SQL clarity
