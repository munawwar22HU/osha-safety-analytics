WITH inspections AS (
    SELECT * FROM {{ ref('stg_osha__inspections') }}
),

violations_summary AS (
    SELECT
        inspection_id,
        COUNT(*)                                           AS total_violations,
        SUM(current_penalty_amount)                        AS total_penalty,
        SUM(CASE WHEN is_serious THEN 1 ELSE 0 END)        AS serious_violations,
        SUM(CASE WHEN is_willful THEN 1 ELSE 0 END)        AS willful_violations,
        SUM(CASE WHEN is_repeat THEN 1 ELSE 0 END)         AS repeat_violations,
        SUM(CASE WHEN abatement_complete THEN 1 ELSE 0 END) AS abated_violations,
        AVG(current_penalty_amount)                        AS avg_penalty_per_violation
    FROM {{ ref('stg_osha__violations') }}
    GROUP BY inspection_id
)

SELECT
    i.inspection_id,
    i.establishment_name,
    i.city,
    i.state_code,
    i.zip_code,
    i.naics_code,
    i.naics_status,
    i.sic_code,
    i.establishment_size_band,
    i.employee_count,
    i.owner_type,
    i.inspection_type,
    i.safety_or_health,
    i.open_date,
    i.close_case_date,
    i.inspection_year,
    i.inspection_month,
    i.inspection_duration_days,
    i.dates_are_valid,

    -- Violation counts
    COALESCE(v.total_violations, 0)      AS total_violations,
    COALESCE(v.total_penalty, 0)         AS total_penalty_amount,
    COALESCE(v.serious_violations, 0)    AS serious_violations,
    COALESCE(v.willful_violations, 0)    AS willful_violations,
    COALESCE(v.repeat_violations, 0)     AS repeat_violations,
    COALESCE(v.abated_violations, 0)     AS abated_violations,
    COALESCE(v.avg_penalty_per_violation, 0) AS avg_penalty_per_violation,

    -- Inspection severity category
    CASE
        WHEN COALESCE(v.total_violations, 0) = 0 THEN 'No Violations'
        WHEN COALESCE(v.willful_violations, 0) > 0 THEN 'Willful'
        WHEN COALESCE(v.serious_violations, 0) > 0 THEN 'Serious'
        WHEN COALESCE(v.repeat_violations, 0) > 0 THEN 'Repeat'
        ELSE 'Low Severity'
    END                                  AS inspection_severity_category,

    -- Has violations flag
    CASE
        WHEN COALESCE(v.total_violations, 0) > 0 THEN TRUE
        ELSE FALSE
    END                                  AS has_violations

FROM inspections i
LEFT JOIN violations_summary v
    ON i.inspection_id = v.inspection_id