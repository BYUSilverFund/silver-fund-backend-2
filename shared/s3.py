import boto3
import pandas as pd
from io import StringIO
import os

class S3:

    def __init__(self) -> None:
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-west-2'
        )

    def get_file(self, bucket_name: str, file_key: str) -> pd.DataFrame:

        s3_object = self.s3.get_object(Bucket=bucket_name, Key=file_key)

        file_content = s3_object['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(file_content))

        return df