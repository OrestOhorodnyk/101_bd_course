import datetime
import os
from fastavro import writer, parse_schema
import pandas as pd


def get_csv_files(path_to_dir: str) -> list:
    return [f for f in os.listdir(path_to_dir) if '.csv.gz' in f]


def create_avro_schema(csv_file_name: str, path_to_dir: str) -> dict:
    """
    :param csv_file_name: string with file name
    :param path_to_dir: string with path to input dir
    :return: parsed avro schema
    """
    schema_template = {
        "namespace": None,
        "type": "record",
        "name": "foo",
        "fields": []
    }
    # read column names form input file csv
    column_names = pd.read_csv(f'{path_to_dir}/{csv_file_name}', nrows=0, compression='gzip', error_bad_lines=False)
    fields = [{"name": f, "type": "string"} for f in list(column_names.columns.values)]
    # add required files to avro schema
    schema_template['fields'] = fields
    schema_template['namespace'] = f"{csv_file_name.split('.')[0]}.avro"
    return parse_schema(schema_template)


def write_csv_to_avro(file_name: str, schema: dict, out_dir: str, in_dir: str, chunk_size: int):
    """
    :param file_name: str with file name
    :param schema: avro schema
    :param out_dir: path to directory to store avro file
    :param in_dir: path to directory with input file
    :param chunk_size: hunk size is used to specify number of record to read and write to process large files
    """
    t = datetime.datetime.now()
    print(f'{file_name} start time {t}')

    count = 0

    # open file to write bytes and read file 'wb+',
    # file opened using context manager 'with', which will close file automatically
    with open(f"{out_dir}/{file_name.split('.')[0]}.avro", "wb+") as out:
        # read chunk of rows fron input file
        # paramete compression='gzip' allows to read csv.gz compressed files
        # chunksize=chunk_size specifies number of rows to read
        # set string as data type for all columns
        for df in pd.read_csv(f'{in_dir}/{file_name}', compression='gzip', chunksize=chunk_size, dtype=str):
            # drop all rows which contains NaN values
            df.dropna(how='any', thresh=None, inplace=True)
            # write rows to avro file using the schema
            writer(out, schema, df.to_dict('records'))
            count += len(df.to_dict('records'))
            print(f'{"*" * 10} {count} of rows processed, file name {file_name}, time {datetime.datetime.now() - t} {"*" * 10}')

    print(f'{file_name} end time {datetime.datetime.now() - t}')
