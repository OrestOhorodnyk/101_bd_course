import os
import shutil
from fastavro import parse_schema, reader, is_avro
from csv_to_avro.main.helpers import get_csv_files, create_avro_schema, write_csv_to_avro


input_dir = f'{os.getcwd()}/test/fixtures/in'
output_dir = f'{os.getcwd()}/test/fixtures/out'

expected_schema = {'namespace': 'sample_submission.avro', 'type': 'record', 'name': 'foo',
                   'fields': [{'name': 'id', 'type': 'string'}, {'name': 'hotel_cluster', 'type': 'string'}]}


def test_get_csv_files():
    assert get_csv_files(input_dir) == ['sample_submission.csv.gz']


def test_create_avro_schema():
    schema = create_avro_schema('sample_submission.csv.gz', input_dir)
    assert schema == parse_schema(expected_schema)


def test_write_csv_to_avro():
    shutil.rmtree(output_dir, ignore_errors=True)
    os.mkdir(output_dir)
    try:
        write_csv_to_avro(
            file_name='sample_submission.csv.gz',
            schema=parse_schema(expected_schema),
            out_dir=output_dir, in_dir=input_dir,
            chunk_size=20000
        )

        # check if file has been created
        assert 'sample_submission.avro' in [f for f in os.listdir(output_dir)]

        # check if outpyt file is avro
        assert is_avro(f'{output_dir}/sample_submission.avro')

        with open(f'{output_dir}/sample_submission.avro', 'rb') as fo:
            # check number of records in file
            assert len(list(reader(fo, reader_schema=None))) == 22
    except Exception as e:
        raise e
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)