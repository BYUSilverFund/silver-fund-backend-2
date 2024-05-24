#!/usr/bin/env python3
import sys
from os import path

sys.path.append(path.abspath(path.join(path.dirname(__file__), '..')))

from config import config
from extractor import ibkr_query, fred_query
from transformer import transform, transform_rf, transform_bmk
from database.database import Database


# Entry point for chron job
def main():
    database = Database()

    funds = config.keys()

    for fund in funds:
        token = config[fund]['token']
        query_types = config[fund]['queries'].keys()

        print(f"Beginning {fund} fund ETL")

        for query_type in query_types:
            query_id = config[fund]['queries'][query_type]

            # Extract
            print(f"Executing IBKR {query_type} query")
            raw_data = ibkr_query(token, query_id)

            # Transform
            print(f"Transforming the {query_type} data")
            transformed_data = transform(raw_data, fund, query_type)

            # Load
            database.load_df(transformed_data, query_type)
            print(f"Data loaded into the {query_type} table in the database.")

    print("Updating risk free rate")
    raw_rf = fred_query()
    transformed_rf = transform_rf(raw_rf)
    database.load_df(transformed_rf, 'risk_free_rate')

    print("Updating benchmark")
    bmk_token = config['undergrad']['token']
    bmk_query_id = config['undergrad']['queries']['positions']

    raw_bmk = ibkr_query(bmk_token, bmk_query_id)
    transformed_bmk = transform_bmk(raw_bmk)
    database.load_df(transformed_bmk, 'benchmark')


if __name__ == "__main__":
    main()
