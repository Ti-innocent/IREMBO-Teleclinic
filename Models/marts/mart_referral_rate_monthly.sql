-- mart_referral_rate_monthly.sql

WITH classified_referrals AS (
    SELECT 
        consultation_id,
        consultation_started_at_utc,
        referral_classification,
        is_doctor_referral,
        is_patient_requested
    FROM {{ ref('int_referrals_classified') }}
),

monthly_summary AS (
    SELECT 
        -- Extract the month from the UTC start time (e.g., '2025-02-01')
        toStartOfMonth(consultation_started_at_utc) AS consultation_month,
        
        count(consultation_id) AS total_completed_consults,
        
        -- Doctor-issued referrals include cases where the doctor issued it independently, 
        -- OR where the patient requested it and the doctor agreed (the 'both' case).
        sum(CASE WHEN is_doctor_referral THEN 1 ELSE 0 END) AS doctor_issued_referrals,
        
        -- Patient-requested referrals that were NOT issued by the doctor
        sum(CASE WHEN referral_classification = 'patient_requested_only' THEN 1 ELSE 0 END) AS patient_requested_only_referrals
        
    FROM classified_referrals
    GROUP BY consultation_month
)

SELECT 
    consultation_month,
    total_completed_consults,
    
    doctor_issued_referrals,
    patient_requested_only_referrals,
    
    -- Doctor-issued referral rate (Comparable across Feb, Mar, Apr)
    round(doctor_issued_referrals / total_completed_consults * 100, 1) AS doctor_issued_referral_rate_pct,
    
    -- Patient-requested (but not doctor-issued) referral rate
    round(patient_requested_only_referrals / total_completed_consults * 100, 1) AS patient_requested_only_rate_pct

FROM monthly_summary
ORDER BY consultation_month

