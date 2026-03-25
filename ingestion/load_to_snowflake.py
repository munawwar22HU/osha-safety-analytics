import os
import glob
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )


def clean_inspection_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.upper().str.strip()

    # Strip timezone from date columns
    date_cols = ['OPEN_DATE', 'CASE_MOD_DATE', 'CLOSE_CONF_DATE',
                 'CLOSE_CASE_DATE', 'LOAD_DT']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
            df[col] = df[col].dt.tz_localize(None)

    # Force string on identifier columns
    str_cols = ['ACTIVITY_NR', 'REPORTING_ID', 'NAICS_CODE',
                'SIC_CODE', 'SITE_ZIP', 'MAIL_ZIP']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'nan': None, 'None': None, '': None})

    # Replace NAICS_CODE = 0 with None
    if 'NAICS_CODE' in df.columns:
        df['NAICS_CODE'] = df['NAICS_CODE'].replace('0', None)

    # Force numeric on employee count
    if 'NR_IN_ESTAB' in df.columns:
        df['NR_IN_ESTAB'] = pd.to_numeric(df['NR_IN_ESTAB'], errors='coerce')

    # Drop STATE_FLAG — completely empty
    if 'STATE_FLAG' in df.columns:
        df = df.drop(columns=['STATE_FLAG'])

    return df


def clean_violation_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.upper().str.strip()

    # Remove deleted violations
    if 'DELETE_FLAG' in df.columns:
        df = df[df['DELETE_FLAG'].isna()].copy()

    # Strip timezone from date columns
    date_cols = ['ISSUANCE_DATE', 'ABATE_DATE', 'LOAD_DT',
                 'CONTEST_DATE', 'FINAL_ORDER_DATE',
                 'FTA_ISSUANCE_DATE', 'FTA_CONTEST_DATE',
                 'FTA_FINAL_ORDER_DATE']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
            df[col] = df[col].dt.tz_localize(None)

    # Force string on identifier columns
    str_cols = ['ACTIVITY_NR', 'CITATION_ID', 'FTA_INSP_NR']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'nan': None, 'None': None, '': None})

    # Force numeric on penalty and count columns
    numeric_cols = ['CURRENT_PENALTY', 'INITIAL_PENALTY',
                    'FTA_PENALTY', 'NR_INSTANCES', 'NR_EXPOSED', 'GRAVITY']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def load_chunked_files(conn, folder_path: str, table_name: str,
                       clean_fn) -> None:
    files = sorted(glob.glob(os.path.join(folder_path, "*.csv")))
    total_files = len(files)
    logger.info(f"Found {total_files} files in {folder_path}")

    total_rows = 0
    failed_files = []

    for file_num, filepath in enumerate(files):
        filename = os.path.basename(filepath)
        logger.info(f"Processing {file_num + 1}/{total_files}: {filename}")

        try:
            df = pd.read_csv(
                filepath,
                encoding='latin-1',
                low_memory=False,
                on_bad_lines='skip'
            )

            df = clean_fn(df)

            overwrite = (file_num == 0)

            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name,
                auto_create_table=False,
                overwrite=overwrite
            )

            total_rows += nrows
            logger.info(f"Loaded {nrows} rows. Running total: {total_rows}")

        except Exception as e:
            logger.error(f"Failed on {filename}: {e}")
            failed_files.append(filename)
            continue

    if failed_files:
        logger.warning(f"Failed files: {failed_files}")

    logger.info(f"Completed {table_name}. Total rows: {total_rows}")


if __name__ == "__main__":
    conn = get_snowflake_connection()

    try:
        logger.info("=== Loading Inspections ===")
        load_chunked_files(
            conn,
            folder_path="data/raw/inspection",
            table_name="RAW_INSPECTIONS",
            clean_fn=clean_inspection_chunk
        )

        logger.info("=== Loading Violations ===")
        load_chunked_files(
            conn,
            folder_path="data/raw/violation",
            table_name="RAW_VIOLATIONS",
            clean_fn=clean_violation_chunk
        )

        logger.info("=== All data loaded successfully ===")

    finally:
        conn.close()