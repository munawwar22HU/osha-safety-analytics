WITH enriched AS (
    SELECT * FROM {{ ref('int_inspections__enriched') }}
    WHERE inspection_year >= 2000
      AND state_code IS NOT NULL
      AND naics_status = 'VALID'
),

industry_stats AS (
    SELECT
        inspection_year,
        naics_code,
        LEFT(naics_code, 2)                                AS naics_sector_code,

        COUNT(*)                                           AS total_inspections,
        SUM(total_violations)                              AS total_violations,
        SUM(serious_violations)                            AS serious_violations,
        SUM(willful_violations)                            AS willful_violations,
        SUM(repeat_violations)                             AS repeat_violations,
        SUM(total_penalty_amount)                          AS total_penalties,

        ROUND(AVG(total_penalty_amount), 2)                AS avg_penalty_per_inspection,
        ROUND(AVG(total_violations), 2)                    AS avg_violations_per_inspection,

        ROUND(
            SUM(serious_violations) * 100.0
            / NULLIF(SUM(total_violations), 0), 2
        )                                                  AS serious_violation_rate,

        ROUND(
            SUM(repeat_violations) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                                  AS repeat_inspection_rate,

        COUNT(DISTINCT state_code)                         AS states_covered

    FROM enriched
    GROUP BY inspection_year, naics_code
)

SELECT
    *,
    LAG(total_violations) OVER (
        PARTITION BY naics_code
        ORDER BY inspection_year
    )                                                      AS prev_year_violations,

    ROUND(
        (total_violations - LAG(total_violations) OVER (
            PARTITION BY naics_code
            ORDER BY inspection_year
        )) * 100.0 / NULLIF(LAG(total_violations) OVER (
            PARTITION BY naics_code
            ORDER BY inspection_year
        ), 0), 2
    )                                                      AS yoy_violation_change_pct

FROM industry_stats
ORDER BY inspection_year DESC, total_violations DESC