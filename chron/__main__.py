from .config import config
from .extractor import *
from shared.utils import render_sql
from shared.database import Database
from shared.s3 import S3
from datetime import datetime
from .logger import PipelineLogger
# from slack.slack import send_message_to_slack

def main():
    db = Database()
    s3 = S3()
    date = datetime.today().strftime("%Y-%m-%d")
    logger = PipelineLogger()

    # Execute ibkr phase for each fund
    for fund in config.keys():
        logger.info(f"Starting pipeline for {fund} fund")
        token = config[fund]['token']

        try:
            for query in config[fund]['queries'].keys():
                try:
                    query_id = config[fund]['queries'][query]

                    # Query
                    logger.info(f"Executing {query} query for {fund} fund")
                    raw_dataframe = ibkr_query(fund, token, query_id)

                    # Load into S3
                    logger.info(f"Dropping raw {query} file for {fund} fund into S3")
                    file_name = f"{date}/{query}/{date}_{query}_{fund}.csv"
                    s3.drop_file(file_name, 'ibkr-historical-data', raw_dataframe)

                    # Load (this step will create or replace the raw dataframes corresponding stage table)
                    stage_table = f"{date}_{fund.upper()}_{query.upper()}"
                    logger.info(f"Loading raw {query} dataframe into stage table: {stage_table}")
                    db.load_dataframe(raw_dataframe, stage_table)

                    # Clean table (drop duplicate rows, remove extra headers, add fund, add date)
                    xf_table = 'XF_' + stage_table
                    xf_template = f"chron/sql/transform/transform_{query}.sql"
                    xf_params = {'stage_table': stage_table, 'xf_table': xf_table, 'fund': fund}
                    xf_query = render_sql(xf_template, xf_params)
                    logger.info(f"Creating transform table: {stage_table} -> {xf_table}")
                    db.execute_sql(xf_query)

                    # Create core table if it doesn't already exist
                    create_template = f"chron/sql/create/create_{query}.sql"
                    create_query = render_sql(create_template)
                    logger.info(f"Verifying that {query} table exists")
                    db.execute_sql(create_query)

                    # Merge xf table into core table
                    merge_template = f"chron/sql/merge/merge_{query}.sql"
                    merge_params = {'xf_table': xf_table}
                    merge_query = render_sql(merge_template, merge_params)
                    logger.info(f"Merging {stage_table} into {query.upper()} table")
                    db.execute_sql(merge_query)

                    # Clean up (drop temporary tables)
                    drop_query = f""" 
                        DROP TABLE "{stage_table}";
                        DROP TABLE "{xf_table}";
                    """
                    logger.info(f"Dropping {stage_table} and {xf_table} tables ")
                    db.execute_sql(drop_query)

                except Exception as query_error:
                    logger.error(f"An error occurred during the {query} query for {fund}: {str(query_error)}")
                    # send_message_to_slack(f"An error occurred during the {query} query for {fund}: {str(query_error)}")
                    continue  # Continue with the next query

        except Exception as fund_error:
            logger.error(f"An error occurred for the {fund} fund: {str(fund_error)}")
            # send_message_to_slack(f"An error occurred for the {fund} fund: {str(fund_error)}")
            continue  # Continue with the next fund

        # Create logs table if not exists
        create_logs_template = f"chron/sql/create/create_logs.sql"
        create_logs_query = render_sql(create_logs_template)
        db.execute_sql(create_logs_query)

        # Record logs in database
        logs = logger.get_logs()
        insert_logs_template = f"chron/sql/merge/merge_logs.sql"
        insert_logs_params = {'fund': fund, 'date': date, 'logs': logs}
        insert_logs_query = render_sql(insert_logs_template, insert_logs_params)
        db.execute_sql(insert_logs_query)
        logger.info(f"Stored logs for fund: {fund}")
        logger.clear_logs()

    
    # Load risk-free rate and benchmark
    try:

        # Extract
        logger.info("Pulling raw risk free rate from FRED")
        rf_dataframe = fred_query()

        # Create
        create_rf_template = f"chron/sql/create/create_risk_free_rate.sql"
        create_rf_query = render_sql(create_rf_template)
        db.execute_sql(create_rf_query)

        # Stage
        stage_table = f'{date}_risk_free_rate'
        logger.info(f"Loading raw rf into {stage_table}")
        db.load_dataframe(rf_dataframe, stage_table)

        # Merge
        logger.info(f"Merging {stage_table} into RISK_FREE_RATE")
        merge_rf_template = "chron/sql/merge/merge_risk_free_rate.sql"
        merge_rf_params = {'xf_table': stage_table}
        merge_rf_query = render_sql(merge_rf_template, merge_rf_params)
        db.execute_sql(merge_rf_query)

        # Clean Up
        logger.info(f"Dropping table: {stage_table}")
        sql = f'''
        DROP TABLE "{stage_table}"
        '''
        db.execute_sql(sql)

    except Exception as e:
        logger.error(f"An error occurred during the risk free rate step: {str(e)}")
        # send_message_to_slack(f"An error occurred during the risk free rate step: {str(e)}")

    try:

        # Create
        logger.info("Creating benchmark table from grad IWV position data")
        create_rf_template = "chron/sql/create/create_benchmark.sql"
        create_rf_query = render_sql(create_rf_template)
        db.execute_sql(create_rf_query)

    except Exception as e:
        logger.error(f"An error occurred during the benchmark step: {str(e)}")
        # send_message_to_slack(f"An error occurred during the benchmark step: {str(e)}")

    try:
        # Create
        logger.info("Creating calendar table")
        cal_dataframe = calendar_query()
        db.load_dataframe(cal_dataframe, "calendar")

    except Exception as e:
        logger.error(f"An error occurred during the calendar step: {str(e)}")
        # send_message_to_slack(f"An error occurred during the calendar step: {str(e)}")

    # send_message_to_slack("Chron pipeline has completed successfully")

if __name__ == '__main__':
    main()
