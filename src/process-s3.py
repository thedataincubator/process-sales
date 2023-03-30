import os
import boto3
import pandas as pd
import numpy as np

SESSION = boto3.session.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)


def analyze(input_filename, output_filename):
    df = pd.read_csv(input_filename)
    results = (df.groupby('product_line')
               .agg({'sale': [('sale mean', np.mean), ('sale std', np.std)],
                     'quantity_ordered': [('total quantity', np.sum)]}))
    results.columns = results.columns.get_level_values(1)
    results.to_csv(output_filename)


def download_from_bucket(bucket_name, filename_in_bucket, filename):
    s3_resource = SESSION.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    data = bucket.download_file(
        Key=filename_in_bucket,
        Filename=filename
    )


def process(bucket_name, filename):
    if not os.path.exists('data'):
        os.mkdir('data')
    download_filename = os.path.join("data", "data_from_bucket.csv")
    download_from_bucket(bucket_name, filename, download_filename)
    analyze(download_filename, os.path.join("data", 'results.csv'))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('bucket_name', type=str, help="s3 bucket name")
    parser.add_argument('filename', type=str, help="Name of file in s3 bucket")
    args = parser.parse_args()

    process(args.bucket_name, args.filename)
