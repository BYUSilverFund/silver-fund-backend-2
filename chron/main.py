from .config import config
from .extractor import ibkr_query, fred_query
from .transformer import transform, transform_rf, transform_bmk
from database.database import Database


# Entry point for chron job
def run():
    database = Database()

    funds = config.keys()

    cron_log_string = ""

    for fund in funds:
        token = config[fund]['token']
        query_types = config[fund]['queries'].keys()

        # print(f"Beginning {fund} fund ETL")
        cron_log_string += f"{fund}: "

        for query_type in query_types:
            try:
                query_id = config[fund]['queries'][query_type]

                # Extract
                # print(f"Executing IBKR {query_type} query")
                cron_log_string += f"{query_type} extracted, "
                raw_data = ibkr_query(token, query_id)

                # Transform
                # print(f"Transforming the {query_type} data")
                cron_log_string += f"transformed, "
                transformed_data = transform(raw_data, fund, query_type)

                # Load
                database.load_df(transformed_data, query_type)
                cron_log_string += f"loaded. \n"
                # print(f"Data loaded into the {query_type} table in the database.")

            except Exception as e:
                # print(f"Error loading {fund} {query_type} data: {e}")
                cron_log_string += f"Error loading {fund} {query_type} data: {e}\n"

    try:
        # print("Updating risk free rate")
        cron_log_string += "Updated risk-free rate.\n"
        raw_rf = fred_query()
        transformed_rf = transform_rf(raw_rf)
        database.load_df(transformed_rf, 'risk_free_rate')

        # print("Updating benchmark")
        cron_log_string += "Updated benchmark.\n"
        bmk_token = config['undergrad']['token']
        bmk_query_id = config['undergrad']['queries']['positions']

        raw_bmk = ibkr_query(bmk_token, bmk_query_id)
        transformed_bmk = transform_bmk(raw_bmk)
        database.load_df(transformed_bmk, 'benchmark')

    except Exception as e:
        # print(f"Error updating risk free rate or benchmark: {e}")
        cron_log_string += f"Error updating risk free rate or benchmark: {e}\n"

    database.load_cron_log(cron_log_string)


