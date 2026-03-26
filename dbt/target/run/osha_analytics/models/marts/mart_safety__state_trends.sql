
  
    

create or replace transient table OSHA_DB.MARTS.mart_safety__state_trends
    
    
    
    as (WITH enriched AS (
    SELECT * FROM OSHA_DB.INTERMEDIATE.int_inspections__enriched
    WHERE inspection_year >= 2000
      AND state_code IS NOT NULL
),

state_yearly AS (
    SELECT
        inspection_year,
        state_code,

        COUNT(*)                                           AS total_inspections,
        SUM(total_violations)                              AS total_violations,
        SUM(serious_violations)                            AS serious_violations,
        SUM(willful_violations)                            AS willful_violations,
        SUM(total_penalty_amount)                          AS total_penalties,

        ROUND(AVG(total_penalty_amount), 2)                AS avg_penalty,

        ROUND(
            SUM(serious_violations) * 100.0
            / NULLIF(SUM(total_violations), 0), 2
        )                                                  AS serious_violation_rate,

        COUNT(DISTINCT CASE
            WHEN inspection_severity_category IN ('Willful', 'Serious')
            THEN inspection_id END
        )                                                  AS high_severity_inspections

    FROM enriched
    GROUP BY inspection_year, state_code
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY inspection_year
        ORDER BY total_violations DESC
    )                                                      AS violation_rank,

    RANK() OVER (
        PARTITION BY inspection_year
        ORDER BY serious_violation_rate DESC
    )                                                      AS serious_rate_rank,

    LAG(total_violations) OVER (
        PARTITION BY state_code
        ORDER BY inspection_year
    )                                                      AS prev_year_violations,

    ROUND(
        (total_violations - LAG(total_violations) OVER (
            PARTITION BY state_code
            ORDER BY inspection_year
        )) * 100.0 / NULLIF(LAG(total_violations) OVER (
            PARTITION BY state_code
            ORDER BY inspection_year
        ), 0), 2
    )                                                      AS yoy_change_pct

FROM state_yearly
ORDER BY inspection_year DESC, violation_rank
    )
;


  