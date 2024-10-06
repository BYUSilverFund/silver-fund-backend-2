import sys
import os
from datetime import datetime
import pandas as pd

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from chron2.utils import clean_ibkr_dataframe, render_sql, get_fund
from shared.database import Database

db = Database()
date = datetime.today().strftime("%Y-%m-%d")

def main(file, table):
    print(f"Loading {file}")

    # Raw
    raw_dataframe = pd.read_csv(file)

    # Clean
    clean_dataframe = clean_ibkr_dataframe(raw_dataframe)

    if clean_dataframe.empty:
        print(f"No data!")
        return

    # Transform
    client_account_id = clean_dataframe['ClientAccountID'].iloc[0]
    fund = get_fund(client_account_id)
    clean_dataframe['fund'] = fund

    print(clean_dataframe)

    # # Load 
    # stage_table = f"{date}_{fund}_{table}"
    # db.load_dataframe(clean_dataframe, stage_table)

    # # Create core table if it doesn't already exist
    # create_template = f"../chron2/sql/create/create_{table}.sql"
    # create_query = render_sql(create_template)
    # db.execute_sql(create_query)

    # print(f"Merging {file} into {table} table as {fund}.")
    
    # # Merge
    # merge_template = f"../chron2/sql/merge/merge_{table}.sql"
    # merge_params = {'stage_table': stage_table}
    # merge_query = render_sql(merge_template, merge_params)
    # db.execute_sql(merge_query)

    # # Clean Up
    # drop_query = f""" 
    #     DROP TABLE "{stage_table}";
    # """
    # db.execute_sql(drop_query)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        file_path = sys.argv[1]
        table = sys.argv[2]
        main(file_path, table)
    else:
        print("File path or table name missing")
