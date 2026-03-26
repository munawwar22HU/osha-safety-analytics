WITH source AS (
    SELECT * FROM {{ source('osha_raw', 'RAW_VIOLATIONS') }}
),

cleaned AS (
    SELECT
        -- Composite primary key
        ACTIVITY_NR || '-' || CITATION_ID                AS violation_key,

        -- Foreign key to inspections
        ACTIVITY_NR                                       AS inspection_id,

        -- Citation identifier
        CITATION_ID                                       AS citation_id,

        -- Violation classification
        VIOL_TYPE                                         AS violation_type_code,
        CASE VIOL_TYPE
            WHEN 'S' THEN 'Serious'
            WHEN 'W' THEN 'Willful'
            WHEN 'R' THEN 'Repeat'
            WHEN 'O' THEN 'Other'
            WHEN 'U' THEN 'Unclassified'
            WHEN 'P' THEN 'Petition to Modify'
            ELSE 'Unknown'
        END                                               AS violation_type_desc,

        -- Severity flags derived from VIOL_TYPE
        CASE WHEN VIOL_TYPE = 'S' THEN TRUE ELSE FALSE END AS is_serious,
        CASE WHEN VIOL_TYPE = 'W' THEN TRUE ELSE FALSE END AS is_willful,
        CASE WHEN VIOL_TYPE = 'R' THEN TRUE ELSE FALSE END AS is_repeat,

        -- Abatement
        CASE WHEN ABATE_COMPLETE = 'X'
             THEN TRUE ELSE FALSE END                     AS abatement_complete,

        -- Penalty amounts
        CASE
            WHEN INITIAL_PENALTY IS NULL THEN NULL
            WHEN INITIAL_PENALTY < 0 THEN 0
            ELSE INITIAL_PENALTY
        END                                               AS initial_penalty_amount,

        CASE
            WHEN CURRENT_PENALTY IS NULL THEN NULL
            WHEN CURRENT_PENALTY < 0 THEN 0
            ELSE CURRENT_PENALTY
        END                                               AS current_penalty_amount,

        CASE
            WHEN FTA_PENALTY IS NULL THEN NULL
            WHEN FTA_PENALTY < 0 THEN 0
            ELSE FTA_PENALTY
        END                                               AS fta_penalty_amount,

        -- Penalty reduction percentage
        CASE
            WHEN INITIAL_PENALTY > 0
             AND CURRENT_PENALTY IS NOT NULL
            THEN ROUND(
                (INITIAL_PENALTY - CURRENT_PENALTY)
                / INITIAL_PENALTY * 100, 2
            )
            ELSE NULL
        END                                               AS penalty_reduction_pct,

        -- Instance counts
        CASE
            WHEN NR_INSTANCES IS NULL THEN NULL
            WHEN NR_INSTANCES < 0 THEN NULL
            ELSE NR_INSTANCES
        END                                               AS instance_count,

        CASE
            WHEN NR_EXPOSED IS NULL THEN NULL
            WHEN NR_EXPOSED < 0 THEN NULL
            ELSE NR_EXPOSED
        END                                               AS employees_exposed,

        -- Gravity score
        GRAVITY                                           AS gravity_score,

        -- Standard cited
        TRIM(STANDARD)                                    AS standard_cited,

        -- Dates converted from nanoseconds
        TO_TIMESTAMP_NTZ(ISSUANCE_DATE / 1000000000)     AS issuance_date,
        TO_TIMESTAMP_NTZ(ABATE_DATE / 1000000000)        AS abatement_date,
        TO_TIMESTAMP_NTZ(LOAD_DT / 1000000000)           AS load_date

    FROM source
    WHERE ACTIVITY_NR IS NOT NULL
      AND CITATION_ID IS NOT NULL
      AND DELETE_FLAG IS NULL
)

SELECT * FROM cleaned