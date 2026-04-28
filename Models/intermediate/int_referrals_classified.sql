-- int_referrals_classified.sql

WITH consultations AS (
    SELECT 
        consultation_id,
        consultation_started_at_utc,
        patient_id
    FROM {{ ref('stg_consultations_fixed') }}
),

clinical_outcomes AS (
    SELECT 
        consultation_id,
        referral_issued  -- boolean
    FROM {{ ref('stg_clinical_outcomes') }}
),

intake_flags AS (
    SELECT 
        consultation_id,
        referral_requested -- boolean
    FROM {{ ref('stg_consultation_requests') }}
),

joined_data AS (
    SELECT
        c.consultation_id,
        c.consultation_started_at_utc,
        c.patient_id,
        coalesce(co.referral_issued, false) AS is_doctor_referral,
        coalesce(i.referral_requested, false) AS is_patient_requested
    FROM consultations c
    LEFT JOIN clinical_outcomes co 
        ON c.consultation_id = co.consultation_id
    LEFT JOIN intake_flags i 
        ON c.consultation_id = i.consultation_id
)

SELECT
    consultation_id,
    consultation_started_at_utc,
    patient_id,
    is_doctor_referral,
    is_patient_requested,
    
    -- Classifies each completed consultation as one of: 
    -- doctor_referral, patient_requested_only, both, or no_referral
    CASE 
        WHEN is_doctor_referral AND is_patient_requested THEN 'both'
        WHEN is_doctor_referral AND NOT is_patient_requested THEN 'doctor_referral'
        WHEN NOT is_doctor_referral AND is_patient_requested THEN 'patient_requested_only'
        ELSE 'no_referral'
    END AS referral_classification
    
FROM joined_data
