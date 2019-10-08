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

	# store dataframe
	readcsvdata = pd.read_csv(csvdata_filepath, delimiter = ",")

	# print dataframe
	print(readcsvdata)

	# store filtered (companies domain name) dataframe
	filtered_output = readcsvdata[pd.notnull(readcsvdata.domain)]

	# print dataframe
	filtered_output

	# convert to json format and store dataframe
	output_jsonformat = filtered_output.to_json("../data/companies_sorted.json")

	# convert to parquet format and store dataframe
	output_parquetformat = filtered_output.to_parquet("../data/companies_sorted.parquet.gzip", compression="gzip")
	
	# convert to avro format and strore dataframe
	# output_avroformat = pdx.to_avro("../data/companies_sorted.avro", filtered_output)
	
	# upload files to the new bucket
	s3_client.upload_file("../data/companies_sorted.csv", bucket, "companies_sorted.csv")
	s3_client.upload_file("../data/companies_sorted.json", bucket, "companies_sorted.json")
	s3_client.upload_file("../data/companies_sorted.parquet.gzip", bucket, "companies_sorted.parquet.gzip")
	# s3_client.upload_file("../data/companies_sorted.avro", bucket, "companies_sorted.avro")
    
if __name__ == "__main__":
    main()
