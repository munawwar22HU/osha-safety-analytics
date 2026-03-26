WITH violations AS (
    SELECT * FROM {{ ref('stg_osha__violations') }}
),

inspections AS (
    SELECT
        inspection_id,
        inspection_year,
        inspection_month,
        state_code,
        naics_code,
        naics_status,
        establishment_size_band,
        owner_type,
        inspection_type,
        safety_or_health
    FROM {{ ref('stg_osha__inspections') }}
)

SELECT
    v.violation_key,
    v.inspection_id,
    v.citation_id,
    v.violation_type_code,
    v.violation_type_desc,
    v.is_serious,
    v.is_willful,
    v.is_repeat,
    v.abatement_complete,
    v.initial_penalty_amount,
    v.current_penalty_amount,
    v.fta_penalty_amount,
    v.penalty_reduction_pct,
    v.instance_count,
    v.employees_exposed,
    v.gravity_score,
    v.standard_cited,
    v.issuance_date,
    v.abatement_date,

    -- Context from inspections
    i.inspection_year,
    i.inspection_month,
    i.state_code,
    i.naics_code,
    i.naics_status,
    i.establishment_size_band,
    i.owner_type,
    i.inspection_type,
    i.safety_or_health,

    -- Penalty size band
    CASE
        WHEN v.current_penalty_amount IS NULL THEN 'No Penalty'
        WHEN v.current_penalty_amount = 0 THEN 'Zero Penalty'
        WHEN v.current_penalty_amount < 1000 THEN 'Under $1K'
        WHEN v.current_penalty_amount < 10000 THEN '$1K-$10K'
        WHEN v.current_penalty_amount < 100000 THEN '$10K-$100K'
        ELSE 'Over $100K'
    END                                AS penalty_size_band

FROM violations v
LEFT JOIN inspections i
    ON v.inspection_id = i.inspection_id