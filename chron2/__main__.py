from .config import config
from .extractor import *
from .utils import clean_ibkr_dataframe, render_sql
from shared.database import Database
from datetime import datetime
from .logger import PipelineLogger

def main():
    db = Database()
    date = datetime.today().strftime("%Y-%m-%d")
    logger = PipelineLogger()

    try:
        # Execute ibkr phase for each fund
        for fund in config.keys():

            logger.info(f"Starting pipeline for {fund} fund")
            token = config[fund]['token']

            for query in config[fund]['queries'].keys():

                query_id = config[fund]['queries'][query]

                # Query
                logger.info(f"Executing {query} query for {fund} fund")
                raw_dataframe = ibkr_query(fund, token, query_id)

                # Clean
                clean_dataframe = clean_ibkr_dataframe(raw_dataframe, query, fund)
                logger.info(f"Cleaned raw {query} dataframe")

                # Load (this step will create or replace the raw dataframes corresponding stage table)
                stage_table = f"{date}_{fund}_{query}"
                logger.info(f"Loading cleaned {query} dataframe into stage table: {stage_table}")
                db.load_dataframe(clean_dataframe, stage_table)

                # Create core table if it doesn't already exist
                create_template = f"chron2/sql/create/create_{query}.sql"
                create_query = render_sql(create_template)
                logger.info(f"Verifying that {query} table exists")
                db.execute_sql(create_query)

                # Merge
                merge_template = f"chron2/sql/merge/merge_{query}.sql"
                merge_params = {'stage_table': stage_table}
                merge_query = render_sql(merge_template, merge_params)
                logger.info(f"Merging {stage_table} into {query} table")
                db.execute_sql(merge_query)

                # Clean Up
                drop_query = f""" 
                    DROP TABLE "{stage_table}";
                """
                logger.info(f"Dropping table {stage_table}")
                db.execute_sql(drop_query)

        # Record logs in database
        logger.info("Pipeline execution completed successfully.")
        logs = logger.get_logs()
        log_query = f"""
            INSERT INTO logs (date, fund, logs) 
            VALUES ('{date}', '{fund}', '{logs}')
        """
        db.execute_sql(log_query)
        logger.info(f"Stored logs for fund: {fund}")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()
