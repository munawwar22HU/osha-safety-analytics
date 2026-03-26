# Data Quality Issues Log

## Sprint 1 — Data Load Summary
- Inspections loaded: 5,165,847 rows, 0 duplicates
- Violations loaded: 12,752,267 rows, 0 duplicates
- Load method: Python chunked ingestion via snowflake-connector write_pandas
- Both tables confirmed clean on primary key uniqueness

---

## Sprint 2 — Data Quality Assessment

### Issue 1: Dates Stored as Nanosecond Integers
- Affected tables: RAW_INSPECTIONS, RAW_VIOLATIONS
- All date columns affected
- Root cause: Pandas stripped timezone info and Snowflake stored
  the underlying nanosecond integer rather than a readable timestamp
- Volume: All date columns across both tables
- Resolution: Use TO_TIMESTAMP_NTZ(column / 1000000000) in all
  dbt staging models to convert back to readable timestamps
- Severity: High — affects all date-based analysis

---

### Issue 2: Invalid Date Sequences
- Affected table: RAW_INSPECTIONS
- Volume: 458 records (0.009% of total)
- Description: CLOSE_CASE_DATE is earlier than OPEN_DATE,
  which is logically impossible
- Root cause: Likely manual data entry errors in older records
- Resolution: Add dates_are_valid boolean flag in staging.
  Do not drop records — they remain useful for non-date analysis.
- Severity: Low — negligible volume

---

### Issue 3: Invalid and Non-Standard State Codes
- Affected table: RAW_INSPECTIONS
- Volume: approximately 983 records total
  - PI: 707 records
  - Null: 166 records
  - UK: 48 records
  - JQ: 26 records
  - CZ: 19 records
  - FN: 16 records
  - MQ: 1 record

#### Root Cause Analysis
- PI (1975-1996): Pre-standardization era. Almost certainly
  Puerto Rico entered using a non-standard code before PR
  was formally adopted. Safe to map to PR.
- JQ, CZ, FN, MQ (1985-1996): Concentrated in the 1980s and
  early 1990s. Internal OSHA jurisdiction codes from an era
  before data entry standards were enforced. Safe to null out.
- UK (2014-2026): Recent records spanning 2014 to 2026.
  Likely OSHA inspections of US federal contractors or military
  installations overseas in the United Kingdom. OSHA has
  authority over some federal employees abroad. Safe to null out.
- Null (2011-2020): Recent records with no state populated.
  Likely inspections where site location was disputed or
  the inspection was conducted remotely.

#### Resolution
  - Map PI to PR (Puerto Rico)
  - Null out UK, JQ, CZ, FN, MQ
  - Flag all nulls as UNKNOWN in staging
- Severity: Low — negligible volume relative to 5.1M total

---

### Issue 4: NAICS Code Coverage Gap
- Affected table: RAW_INSPECTIONS
- Volume breakdown:
  - Valid (6 digit numeric): 2,108,232 records (40.81%)
  - Missing/null: 2,079,574 records (40.26%)
  - Too short (less than 4 digits): 978,041 records (18.93%)
- Root cause: NAICS codes were adopted in 1997 replacing the
  older SIC code system. Pre-1997 inspections have no NAICS
  code. Too short codes represent partial or transitional
  era entries from the late 1990s.
- Resolution:
  - Valid codes: keep as-is, truncate to 6 digits
  - Too short: flag as UNKNOWN
  - Missing/null: flag as UNKNOWN
  - Use SIC_CODE as fallback industry indicator for older records
  - Limit industry-based analysis to post-2000 inspections
    where NAICS coverage is substantially higher
- Severity: High — affects 59% of records for industry analysis

---

### Issue 5: Owner Type Nulls
- Affected table: RAW_INSPECTIONS
- Volume: 875,689 null records (16.95%)
- Full distribution:
  - A (Private): 3,927,007 (76.02%)
  - Null: 875,689 (16.95%)
  - B (Local Govt): 258,647 (5.01%)
  - C (State Govt): 61,870 (1.20%)
  - D (Federal Govt): 42,634 (0.83%)
- Root cause: Older records did not consistently capture
  ownership type during data entry
- Resolution: Map nulls to UNKNOWN in staging. Retain all records.
- Severity: Medium — affects ownership sector analysis

---

### Issue 6: Inspection Type Distribution and Nulls
- Affected table: RAW_INSPECTIONS
- Volume: 11 null records (negligible)
- Full distribution:
  - H (Health): 2,779,268 (53.80%)
  - B (Complaint): 964,599 (18.67%)
  - C (Referral): 390,020 (7.55%)
  - F (Follow-up): 316,604 (6.13%)
  - G (Planned): 236,516 (4.58%)
  - I (Unprog Related): 199,186 (3.86%)
  - A (Accident): 195,404 (3.78%)
  - M (Planned): 27,724 (0.54%)
  - D (Monitoring): 18,267 (0.35%)
  - K: 17,073 (0.33%)
  - J (Unprog Related): 11,084 (0.21%)
  - L (Planned): 9,330 (0.18%)
  - E (Variance): 529 (0.01%)
  - N (Planned): 232 (0.00%)
  - Null: 11 (0.00%)
- Resolution: Map all codes to plain English descriptions
  in staging. Null records flagged as UNKNOWN.
- Severity: Low

---

### Issue 7: Penalty Distribution and Zero Penalties
- Affected table: RAW_VIOLATIONS
- Total violations with non-null penalty: 6,098,376
- Zero penalty violations: 711,590 (11.67% of non-null)
- Penalty statistics:
  - Minimum: $0
  - Maximum: $3,017,000
  - Average: $1,061.49
  - Median: $315
- Root cause: Zero penalty is legitimate — some violation
  types particularly Other carry no mandatory fine.
  Null penalties represent violations where no financial
  assessment was recorded.
- Resolution: Distinguish zero penalty from null penalty
  explicitly in staging. Both are valid analytical states.
- Severity: Low — expected behavior confirmed by domain knowledge

---

### Issue 8: Violation Type Distribution and Penalty Gradient
- Affected table: RAW_VIOLATIONS
- Distribution and average penalties:
  - O (Other): 6,690,466 (52.46%), avg $366
  - S (Serious): 5,743,257 (45.04%), avg $1,025
  - R (Repeat): 262,008 (2.05%), avg $3,265
  - W (Willful): 46,578 (0.37%), avg $18,184
  - U (Unclassified): 9,931 (0.08%), avg $18,530
  - Null: 16 records (negligible)
- Note: The penalty gradient validates data integrity.
  Willful violations correctly carry the highest average fines,
  confirming the data is logically consistent across violation
  severity levels.
- Resolution: Map codes to full descriptions in staging.
  Null records flagged as UNKNOWN.
- Severity: Informational — confirms logical data consistency

---

### Issue 9: Inspection Volume Trends
- Affected table: RAW_INSPECTIONS
- Key observations:
  - Peak inspection years: 2009-2012 exceeding 100K per year
  - COVID-19 impact visible: 2020-2022 show significant decline
  - 2026 is partial year with 13,984 inspections to date
  - Pre-2000 data has significant NAICS code gaps
  - Data spans from 1970 to present covering 55+ years
- Resolution:
  - For trend analysis: filter to 2000 onwards for cleaner data
  - For penalty analysis: filter to 1990 onwards
  - For industry analysis: filter to 2000 onwards where
    NAICS coverage is substantially higher
  - Document all scope decisions clearly in dashboard
- Severity: Informational

---

### Issue 10: Establishment Size — Zero and Unknown Values
- Affected table: RAW_INSPECTIONS
- Volume:
  - 1-10 employees: 2,305,872 (44.64%)
  - 11-50 employees: 1,414,383 (27.38%)
  - 51-250 employees: 809,004 (15.66%)
  - 250+ employees: 366,046 (7.09%)
  - Unknown/null: 237,926 (4.61%)
  - Zero employees: 32,616 (0.63%)
- Root cause: Zero employee count is a data entry error.
  Establishments cannot have zero employees at time of
  inspection. Unknown records were not captured during entry.
- Resolution: Treat zero the same as null. Both flagged as
  UNKNOWN in the size band classification in staging.
- Severity: Low

---

## Cleaning Rules Summary for Sprint 3 dbt Staging Models

| Column | Issue | Cleaning Rule |
|--------|-------|---------------|
| All date columns | Nanosecond integers | TO_TIMESTAMP_NTZ(col / 1000000000) |
| OPEN_DATE vs CLOSE_CASE_DATE | Invalid sequences | Add dates_are_valid boolean flag |
| SITE_STATE = PI | Wrong code for Puerto Rico | Map to PR |
| SITE_STATE in UK, JQ, CZ, FN, MQ | Foreign or unknown codes | Set to NULL |
| SITE_STATE IS NULL | Missing | Flag as UNKNOWN |
| NAICS_CODE IS NULL | Missing | Flag as UNKNOWN |
| NAICS_CODE length less than 4 | Too short | Flag as UNKNOWN |
| NAICS_CODE = 0 | Zero value | Flag as UNKNOWN |
| OWNER_TYPE IS NULL | Missing | Flag as UNKNOWN |
| INSP_TYPE IS NULL | Missing | Flag as UNKNOWN |
| NR_IN_ESTAB = 0 | Zero employees | Flag as UNKNOWN |
| NR_IN_ESTAB IS NULL | Missing | Flag as UNKNOWN |
| CURRENT_PENALTY = 0 | Zero fine | Keep as zero, distinguish from null |
| VIOL_TYPE IS NULL | Missing | Flag as UNKNOWN |
| DELETE_FLAG IS NOT NULL | Deleted violations | Already filtered during ingestion |


### Issue 11: Violation Primary Key Clarification
- Affected table: RAW_VIOLATIONS
- Finding: CITATION_ID alone has only 6,554 unique values across
  12,752,267 records — it is NOT a standalone unique key
- Root cause: Citation IDs are scoped per inspection. Every
  inspection resets its own citation numbering starting at 01001.
  The same citation ID appears across thousands of inspections.
- Confirmed unique key: ACTIVITY_NR + CITATION_ID composite
  produces 12,752,267 unique combinations — zero duplicates
- Resolution: In dbt staging and all downstream models, always
  reference violations using the composite key
  ACTIVITY_NR || '-' || CITATION_ID, never CITATION_ID alone
- Severity: High — incorrect uniqueness assumption would
  corrupt any join logic in staging and mart models
---

## Sprint 2 Sign-off
All profiling queries executed and documented. Cleaning rules
defined for every identified issue. Ready to proceed to Sprint 3
dbt staging model development.

