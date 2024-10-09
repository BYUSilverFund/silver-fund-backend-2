import boto3
import pandas as pd
from io import StringIO, BytesIO
from dotenv import load_dotenv
import os

class S3:

    def __init__(self) -> None:
        load_dotenv()

        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
            region_name='us-west-2' # Should probably make this an environment variable
        )

    def get_file(self, bucket_name: str, file_key: str) -> pd.DataFrame:

        s3_object = self.s3.get_object(Bucket=bucket_name, Key=file_key)

        file_content = s3_object['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(file_content))

        return df
    
    def drop_file(self, file_name: str, bucket_name: str, file_data: pd.DataFrame) -> None:

        csv_buffer = StringIO()
        file_data.to_csv(csv_buffer, index=False)
        csv_bytes = BytesIO(csv_buffer.getvalue().encode())

        self.s3.upload_fileobj(csv_bytes, bucket_name, file_name)