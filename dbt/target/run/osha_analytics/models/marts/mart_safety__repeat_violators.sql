
  
    

create or replace transient table OSHA_DB.MARTS.mart_safety__repeat_violators
    
    
    
    as (WITH enriched AS (
    SELECT * FROM OSHA_DB.INTERMEDIATE.int_inspections__enriched
    WHERE state_code IS NOT NULL
)

SELECT
    establishment_name,
    state_code,
    naics_code,
    establishment_size_band,
    owner_type,

    COUNT(*)                                               AS total_inspections,
    MIN(open_date)                                         AS first_inspection_date,
    MAX(open_date)                                         AS last_inspection_date,
    SUM(total_violations)                                  AS lifetime_violations,
    SUM(total_penalty_amount)                              AS lifetime_penalties,
    SUM(serious_violations)                                AS lifetime_serious_violations,
    SUM(willful_violations)                                AS lifetime_willful_violations,

    DATEDIFF(
        'day',
        MIN(open_date),
        MAX(open_date)
    )                                                      AS days_between_first_last,

    ROUND(
        SUM(serious_violations) * 100.0
        / NULLIF(SUM(total_violations), 0), 2
    )                                                      AS serious_violation_rate,

    CASE
        WHEN COUNT(*) >= 5 THEN 'Chronic Violator'
        WHEN COUNT(*) >= 3 THEN 'Repeat Violator'
        WHEN COUNT(*) = 2 THEN 'Inspected Twice'
        ELSE 'Single Inspection'
    END                                                    AS violator_category

FROM enriched
GROUP BY
    establishment_name,
    state_code,
    naics_code,
    establishment_size_band,
    owner_type
HAVING COUNT(*) >= 2
ORDER BY total_inspections DESC, lifetime_penalties DESC
    )
;


  