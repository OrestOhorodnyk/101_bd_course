import os
from csv_to_avro.main.helpers import get_csv_files, write_csv_to_avro, create_avro_schema

input_dir = f'{os.getcwd()}/input_data'
output_dir = f'{os.getcwd()}/output_data/'


if __name__ == "__main__":

    for f in get_csv_files(input_dir):

        write_csv_to_avro(
            file_name=f,
            schema=create_avro_schema(f, input_dir),
            out_dir=output_dir,
            in_dir=input_dir,
            chunk_size=10000
        )
