import datetime
import os
from fastavro import writer, parse_schema
import pandas as pd

input_dir = f'{os.getcwd()}/input_data'
output_dir = f'{os.getcwd()}/output_data/'


schema_template = {"namespace": None,
                   "type": "record",
                   "name": "foo",
                   "fields": []
                   }
# chunk size is used to specify number of record to read and write to process large files
chunk_size = 10000


if __name__ == "__main__":
    # get all files from the input_data directory adn convert them fro csv to avro
    for f in os.listdir(input_dir):
        t = datetime.datetime.now()
        print(f'{f} start time {t}')
        # read column names form input file csv
        data = pd.read_csv(f'{input_dir}/{f}', nrows=0, compression='gzip', error_bad_lines=False)
        # form list of fields for avro schema
        fields = [{"name": f, "type": "string"} for f in list(data.columns.values)]

        # add requred filds to avroo schema
        schema_template['fields'] = fields
        schema_template['namespace'] = f"{f.split('.')[0]}.avro"

        # pars schema
        schema = parse_schema(schema_template)

        count = 0
        # open file to write bytes and read file 'wb+',
        # file opened using context manager 'with', which will close file automatically
        with open(f"{output_dir}/{f.split('.')[0]}.avro", "wb+") as out:
            # read chunk of rows fron input file
            # paramete compression='gzip' allows to read csv.gz compressed files
            # chunksize=chunk_size specifies number of rows to read
            # set string as data type for all columns
            for df in pd.read_csv(f'{input_dir}/{f}', compression='gzip',  chunksize=chunk_size,
                                  dtype=str):
                # drop all rows which contains NaN values
                df.dropna(how='any', thresh=None, inplace=True)
                # write rows to avro file using the schema
                writer(out, schema, df.to_dict('records'))
                count += len(df.to_dict('records'))
                print(f'{"*" * 10} {count} of rows processed, file name {f}, time {datetime.datetime.now() - t} {"*" * 10}')
                break
        print(f'{f} end time {datetime.datetime.now() -t }')

