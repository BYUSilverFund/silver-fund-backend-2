import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from shared.utils import render_sql
from shared.database import Database
from shared.s3 import S3
from chron2.logger import PipelineLogger

# Initialize database, s3 bucket, and logger
db = Database()
s3 = S3()
logger = PipelineLogger()

logger.info("Pulling all files from S3")
files = s3.list_files('ibkr-historical-data')

for file in files:

    # Split file path
    [bucket, date, query, file_name] = file.split("/")
    fund = file_name.split('_')[-1].split('.')[0]
    fund = 'brigham_' + fund if fund == 'capital' else fund

    # Pull csv file from s3 bucket
    logger.info(f"Getting {file_name} from S3")
    raw_dataframe = s3.get_file(bucket, f"{date}/{query}/{file_name}")

    # Stage the csv file in the database
    stage_table = f"{date}_{fund.upper()}_{query.upper()}"
    logger.info(f"Loading raw {query} dataframe into stage table: {stage_table}")
    db.load_dataframe(raw_dataframe, stage_table)

    # Create clean table from stage table
    xf_table = 'XF_' + stage_table
    xf_template = f"chron2/sql/transform/transform_{query}.sql"
    xf_params = {'stage_table': stage_table, 'xf_table': xf_table, 'fund': fund}
    xf_query = render_sql(xf_template, xf_params)
    logger.info(f"Creating transform table: {stage_table} -> {xf_table}")
    db.execute_sql(xf_query)

    # Create core table if it doesn't already exist
    create_template = f"chron2/sql/create/create_{query}.sql"
    create_query = render_sql(create_template)
    logger.info(f"Verifying that {query} table exists")
    db.execute_sql(create_query)

    # Merge transform table into core table
    merge_template = f"chron2/sql/merge/merge_{query}.sql"
    merge_params = {'xf_table': xf_table}
    merge_query = render_sql(merge_template, merge_params)
    logger.info(f"Merging {stage_table} into {query.upper()} table")
    db.execute_sql(merge_query)

    # Drop temporary tables
    drop_query = f""" 
        DROP TABLE "{stage_table}";
        DROP TABLE "{xf_table}";
    """
    logger.info(f"Dropping {stage_table} and {xf_table} tables ")
    db.execute_sql(drop_query)
