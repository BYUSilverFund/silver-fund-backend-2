import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from shared.database import Database

db = Database()
def main(fund, table):
    print(f"Downloading {table} table for {fund} fund")

    sql = f"""
    SELECT * FROM {table} WHERE fund = '{fund}' ORDER BY date;
    """

    df = db.get_dataframe(sql)

    df.to_csv(f"download/{table}_{fund}.csv", index=False)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        fund = sys.argv[1]
        table = sys.argv[2]
        main(fund, table)
    else:
        print("Fund or table name missing")