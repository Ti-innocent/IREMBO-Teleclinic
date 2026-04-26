# IREMBO-Teleclinic
Remote assignement for Analytics Engineer
**Assumptions:**
- consultation_started_at may contain mixed timezone formats (UTC and UTC+2 string offsets)
- referral_issued and referral_requested are boolean (0/1)
- one row per consultation_id in staging models

**Key decisions:**
- Standardized all timestamps to UTC before calculating wait times
- Defined valid wait time range as 0–240 minutes
- Separated patient-requested and doctor-issued referrals for metric clarity
- Included "both" category in both aggregates to preserve counts

**Testing:**
- Added anomaly detection for month-over-month referral spikes
- Validated no negative wait times after timezone correction
- Enforced accepted values for referral classification

**AI usage:**
- Used AI for structuring SQL.
