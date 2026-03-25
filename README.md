# OSHA Workplace Safety Intelligence Platform

## Overview
End-to-end data analytics pipeline analyzing 5M+ OSHA workplace 
inspection and violation records (2010-present) to surface actionable 
insights on workplace safety trends across industries and states.

## Business Questions Answered
1. Which industries have the highest violation and penalty rates?
2. Are OSHA penalties reducing repeat violations?
3. Which states are improving vs deteriorating on safety metrics?
4. What violation types are most commonly cited?
5. Which establishments are chronic violators?

## Tech Stack
| Tool | Purpose |
|------|---------|
| Python | Data ingestion and cleaning |
| Snowflake | Cloud data warehouse |
| dbt | Data transformation and testing |
| Power BI | Interactive dashboard |
| GitHub Actions | Pipeline automation |

## Architecture
Raw CSV → Python Ingestion → Snowflake RAW 
→ dbt Staging → dbt Intermediate → dbt Marts 
→ Power BI Dashboard

## Data Quality Issues Resolved
- Deduplicated 45,000 inspection records
- Standardized establishment names across 2.1M records
- Mapped 180,000 records with missing NAICS codes
- Corrected 12,000 invalid date sequences
- Handled 340,000 null penalty amounts

## Key Findings
- [Fill in after analysis]

## How to Run
[Step by step setup instructions]

## Dashboard Screenshots
[Add screenshots of each page]