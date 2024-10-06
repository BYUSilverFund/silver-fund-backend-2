from chron2.utils import clean_ibkr_dataframe, render_sql, get_fund
from shared.database import Database
from datetime import datetime
import sys
import pandas as pd

query = 'positions'

db = Database()
date = datetime.today().strftime("%Y-%m-%d")

def main(file):
    print(f"Loading {file}.")

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

    # Load 
    stage_table = f"{date}_{fund}_{query}"
    db.load_dataframe(clean_dataframe, stage_table)

    print(f"Merging {file} into {query} table as {fund}.")
    
    # Merge
    merge_template = f"chron2/sql/merge/merge_{query}.sql"
    merge_params = {'stage_table': stage_table}
    merge_query = render_sql(merge_template, merge_params)
    db.execute_sql(merge_query)

    # Clean Up
    drop_query = f""" 
        DROP TABLE "{stage_table}";
    """
    db.execute_sql(drop_query)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        main(file_path)
    else:
        print("No file path provided.")
