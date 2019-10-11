import boto3
import numpy as np
import pandas as pd
import fastparquet
#import pandavro as pdx

def main():

    region = "eu-west-1"

    # initialized s3 resource
    s3_client = boto3.client('s3', region_name=region)

    # location constraint for s3 bucket
    location = {'LocationConstraint': region}

    bucket = "blossom-data-eng-gifty-dovie"
    s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)

    # download file to local filesystem
    s3_client.download_file(bucket, "data.csv", '../data/companies_sorted.csv')

    # store file path in a variable
    csvdata_filepath = "../data/companies_sorted.csv"

    # store in variable
    readcsvdata = pd.read_csv(csvdata_filepath, delimiter=",")

    # read dataframe
    readcsvdata

    # store filtered (companies domain name) dataframe
    filtered_output = readcsvdata[pd.notnull(readcsvdata.domain)]

    # write dataframe to file
    filtered_output

    # convert to json format and store dataframe
    output_jsonformat = filtered_output.to_json(
        "../data/companies_sorted.json.gz")

    # write dataframe to file
    output_jsonformat

    # convert to parquet format and store dataframe
    output_parquetformat = filtered_output.to_parquet(
        "../data/companies_sorted.parquet")

    # write dataframe to file
    output_parquetformat

    # convert to avro format and strore dataframe
    # output_avroformat = pdx.to_avro("../data/companies_sorted.avro", filtered_output)

    # write dataframe to file
    # output_avroformat

    # store file path in a variable
    jsondata_filepath = "../data/companies_sorted.json.gz"
    parquetdata_filepath = "../data/companies_sorted.parquet"
    # avrodata_filepath = "../data/companies_sorted.avro"

    # upload files to the new bucket
    s3_client.upload_file(csvdata_filepath, bucket, "companies_sorted.csv")
    s3_client.upload_file(jsondata_filepath, bucket,
                          "companies_sorted.json.gz")
    s3_client.upload_file(parquetdata_filepath, bucket,
                          "companies_sorted.parquet")


    # s3_client.upload_file(avrodata_filepath, bucket, "companies_sorted.avro")
if __name__ == "__main__":
    main()file

