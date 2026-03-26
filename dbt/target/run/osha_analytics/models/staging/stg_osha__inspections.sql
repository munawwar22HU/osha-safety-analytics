
  create or replace   view OSHA_DB.STAGING.stg_osha__inspections
  
  
  
  
  as (
    WITH source AS (
    SELECT * FROM OSHA_DB.RAW.RAW_INSPECTIONS
),

cleaned AS (
    SELECT
        -- Primary key
        ACTIVITY_NR AS inspection_id,

        -- Establishment details
        UPPER(TRIM(ESTAB_NAME))                         AS establishment_name,
        UPPER(TRIM(SITE_CITY))                          AS city,

        -- State cleaning
        CASE
            WHEN UPPER(TRIM(SITE_STATE)) = 'PI' THEN 'PR'
            WHEN UPPER(TRIM(SITE_STATE)) IN (
                'UK','JQ','CZ','FN','MQ'
            ) THEN NULL
            WHEN UPPER(TRIM(SITE_STATE)) IN (
                'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
                'DC','PR','GU','VI','AS','MP'
            ) THEN UPPER(TRIM(SITE_STATE))
            ELSE NULL
        END                                             AS state_code,

        TRIM(SITE_ZIP)                                  AS zip_code,

        -- NAICS code cleaning
        CASE
            WHEN NAICS_CODE IS NULL THEN NULL
            WHEN NAICS_CODE = '0' THEN NULL
            WHEN LENGTH(NAICS_CODE) < 4 THEN NULL
            ELSE LEFT(NAICS_CODE, 6)
        END                                             AS naics_code,

        -- NAICS validity flag
        CASE
            WHEN NAICS_CODE IS NULL THEN 'MISSING'
            WHEN NAICS_CODE = '0' THEN 'MISSING'
            WHEN LENGTH(NAICS_CODE) < 4 THEN 'TOO_SHORT'
            ELSE 'VALID'
        END                                             AS naics_status,

        TRIM(SIC_CODE)                                  AS sic_code,

        -- Establishment size
        CASE
            WHEN NR_IN_ESTAB IS NULL THEN 'UNKNOWN'
            WHEN NR_IN_ESTAB = 0 THEN 'UNKNOWN'
            WHEN NR_IN_ESTAB BETWEEN 1 AND 10 THEN '1-10'
            WHEN NR_IN_ESTAB BETWEEN 11 AND 50 THEN '11-50'
            WHEN NR_IN_ESTAB BETWEEN 51 AND 250 THEN '51-250'
            WHEN NR_IN_ESTAB > 250 THEN '250+'
            ELSE 'UNKNOWN'
        END                                             AS establishment_size_band,

        NR_IN_ESTAB                                     AS employee_count,

        -- Owner type
        CASE OWNER_TYPE
            WHEN 'A' THEN 'Private'
            WHEN 'B' THEN 'Local Government'
            WHEN 'C' THEN 'State Government'
            WHEN 'D' THEN 'Federal Government'
            ELSE 'Unknown'
        END                                             AS owner_type,

        -- Inspection type
        CASE INSP_TYPE
            WHEN 'A' THEN 'Accident'
            WHEN 'B' THEN 'Complaint'
            WHEN 'C' THEN 'Referral'
            WHEN 'D' THEN 'Monitoring'
            WHEN 'E' THEN 'Variance'
            WHEN 'F' THEN 'Follow-Up'
            WHEN 'G' THEN 'Planned'
            WHEN 'H' THEN 'Health'
            WHEN 'I' THEN 'Unprog Related'
            WHEN 'J' THEN 'Unprog Related'
            WHEN 'K' THEN 'Planned'
            WHEN 'L' THEN 'Planned'
            WHEN 'M' THEN 'Planned'
            WHEN 'N' THEN 'Planned'
            ELSE 'Unknown'
        END                                             AS inspection_type,

        CASE SAFETY_HLTH
            WHEN 'S' THEN 'Safety'
            WHEN 'H' THEN 'Health'
            ELSE 'Unknown'
        END                                             AS safety_or_health,

        -- Date conversions from nanoseconds
        TO_TIMESTAMP_NTZ(OPEN_DATE / 1000000000)        AS open_date,
        TO_TIMESTAMP_NTZ(CASE_MOD_DATE / 1000000000)    AS case_mod_date,
        TO_TIMESTAMP_NTZ(CLOSE_CONF_DATE / 1000000000)  AS close_conf_date,
        TO_TIMESTAMP_NTZ(CLOSE_CASE_DATE / 1000000000)  AS close_case_date,
        TO_TIMESTAMP_NTZ(LOAD_DT / 1000000000)          AS load_date,

        -- Date validity flag
        CASE
            WHEN CLOSE_CASE_DATE IS NOT NULL
             AND OPEN_DATE IS NOT NULL
             AND CLOSE_CASE_DATE < OPEN_DATE THEN FALSE
            ELSE TRUE
        END                                             AS dates_are_valid,

        -- Derived fields
        YEAR(TO_TIMESTAMP_NTZ(OPEN_DATE / 1000000000))  AS inspection_year,
        MONTH(TO_TIMESTAMP_NTZ(OPEN_DATE / 1000000000)) AS inspection_month,

        DATEDIFF(
            'day',
            TO_TIMESTAMP_NTZ(OPEN_DATE / 1000000000),
            COALESCE(
                TO_TIMESTAMP_NTZ(CLOSE_CASE_DATE / 1000000000),
                CURRENT_DATE
            )
        )                                               AS inspection_duration_days

    FROM source
    WHERE ACTIVITY_NR IS NOT NULL
      AND OPEN_DATE IS NOT NULL
      AND TO_TIMESTAMP_NTZ(OPEN_DATE / 1000000000) >= '1972-01-01'
      AND TO_TIMESTAMP_NTZ(OPEN_DATE / 1000000000) <= CURRENT_DATE
)

SELECT * FROM cleaned
  );

