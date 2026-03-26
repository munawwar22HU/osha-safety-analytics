
  
    

create or replace transient table OSHA_DB.MARTS.mart_safety__violation_types
    
    
    
    as (WITH violations AS (
    SELECT * FROM OSHA_DB.INTERMEDIATE.int_violations__enriched
    WHERE inspection_year >= 2000
      AND state_code IS NOT NULL
)

SELECT
    inspection_year,
    violation_type_desc,
    state_code,
    naics_code,
    naics_status,
    safety_or_health,

    COUNT(*)                                               AS total_violations,
    SUM(current_penalty_amount)                            AS total_penalties,
    ROUND(AVG(current_penalty_amount), 2)                  AS avg_penalty,
    MAX(current_penalty_amount)                            AS max_penalty,

    SUM(CASE WHEN abatement_complete THEN 1 ELSE 0 END)    AS abated_count,
    ROUND(
        SUM(CASE WHEN abatement_complete THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                                      AS abatement_rate,

    SUM(instance_count)                                    AS total_instances,
    SUM(employees_exposed)                                 AS total_employees_exposed,

    ROUND(AVG(gravity_score), 2)                           AS avg_gravity_score

FROM violations
GROUP BY
    inspection_year,
    violation_type_desc,
    state_code,
    naics_code,
    naics_status,
    safety_or_health
ORDER BY inspection_year DESC, total_violations DESC
    )
;


  