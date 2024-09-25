#!/usr/bin/env python3
from .config import config
from .extractor import *
from .utils import clean_ibkr_dataframe, render_template
from shared.database import Database
from datetime import datetime

def main():
    db = Database()
    date = datetime.today()

    # Execute ibkr phase for each fund
    for fund in config.keys():

        token = config[fund]['token']

        for query in config[fund]['queries'].keys():

            query_id = config[fund]['queries'][query]

            # Query
            raw_dataframe = ibkr_query(fund, token, query_id)

            # Clean
            clean_dataframe = clean_ibkr_dataframe(raw_dataframe, query, fund)
            
            # Load
            stage_table = f"{date}_{fund}_{query}"
            db.load_dataframe(clean_dataframe, stage_table)

            # Transform
            transform_template = f"transform_{query}.sql"
            transform_values = {'xf_table': f"{stage_table}_xf"}
            sql = render_template(transform_template, transform_values)
            db.execute_sql(sql)

            # Merge
            merge_template = f"merge_{query}.sql"
            merge_vales = {'core_table': query}
            sql = render_template(merge_template, merge_vales)
            db.execute_sql(merge_template, merge_vales)

    # Execute fred phase

    pass


if __name__ == '__main__':
    main()
