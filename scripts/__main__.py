from shared.database import Database
from io import StringIO, BytesIO
import pandas as pd
import boto3
import os

FUNDS = ['grad', 'undergrad']

def query_fund_positions(fund: str) -> pd.DataFrame:
    db = Database()

    query = f"""
        SELECT "Symbol", "Description", cast("PositionValue" as decimal) as PositionValue, date, "fund" FROM positions
        WHERE fund = '{fund}'
        ORDER BY date DESC, PositionValue DESC
        LIMIT 10;
    """

    df = pd.read_sql(query, db.engine)

    return df

def upload_fund_positions(df: pd.DataFrame, fund: str) -> None:
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                        region_name='us-west-2')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = BytesIO(csv_buffer.getvalue().encode())
    
    bucket = "silverfund-alumni-info"
    key = f"{fund}_positions.csv"

    s3.upload_fileobj(csv_bytes, bucket, key)


def main() -> None:
    for fund in FUNDS:
        df = query_fund_positions(fund)
        upload_fund_positions(df, fund)

if __name__ == "__main__":
    main()
    
    


    
