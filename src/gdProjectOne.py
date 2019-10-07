import numpy as np
import pandas as pd
import fastparquet
import pandavro as pdx

# store file path in a variable
csvdata_filepath = '/media/gifty_dovie/New Volume/Documents/baP/source/data/companies_sorted.csv'

# store dataframe
readcsvdata = pd.read_csv(csvdata_filepath, delimiter = ',')

# print dataframe
readcsvdata

# store filtered (companies domain name) dataframe
filtered_output = readcsvdata[readcsvdata.domain.notnull()]

# print dataframe
filtered_output

# convert to json format and store dataframe
output_jsonformat = filtered_output.to_json('companies_sorted.json')

# convert to parquet format and store dataframe
output_parquetformat = filtered_output.to_parquet('companies_sorted.parquet.gzip', compression='gzip')

# convert to avro format and strore dataframe
output_avroformat = pdx.to_avro('companies_sorted.avro', filtered_output)

