from shared.database import Database
from shared.utils import render_sql
from datetime import datetime
from io import StringIO, BytesIO
import pandas as pd
import boto3
import os

FUNDS = ['grad', 'undergrad']

def query_fund_positions(fund: str, db: Database) -> pd.DataFrame:

    query = f"""
            WITH latest_date AS (
                SELECT MAX(CALDT) as max_date
                FROM positions
                WHERE fund = '{fund}'
            )
            SELECT TICKER, SHARES * PRICE AS PositionValue, CALDT, FUND
            FROM positions
            WHERE FUND = '{fund}' AND CALDT = (SELECT max_date FROM latest_date)
            ORDER BY PositionValue DESC
            LIMIT 10
            """

    df = pd.read_sql(query, db.engine)

    return df

def upload_fund_positions(df: pd.DataFrame, fund: str) -> None:
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
                        aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
                        region_name='us-west-2')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = BytesIO(csv_buffer.getvalue().encode())
    
    bucket = "silverfund-alumni-info"
    key = f"{fund}_positions.csv"

    s3.upload_fileobj(csv_bytes, bucket, key)


def main() -> None:
    db = Database()
    cron_log_string = ""

    for fund in FUNDS:
        df = query_fund_positions(fund, db)
        upload_fund_positions(df, fund)
        cron_log_string = f"{fund} top positions uploaded to S3.\n"
        date = datetime.today().strftime("%Y-%m-%d")

        insert_logs_template = f"chron/sql/merge/merge_logs.sql"
        insert_logs_params = {'fund': fund, 'date': date, 'logs': cron_log_string}
        insert_logs_query = render_sql(insert_logs_template, insert_logs_params)
        db.execute_sql(insert_logs_query)
    
if __name__ == "__main__":
    main()
    
    


    
