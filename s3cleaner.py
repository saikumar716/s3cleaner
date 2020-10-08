import boto3
import time
import logging
import sys
import pytz
import json
import collections
from datetime import datetime, timedelta
from awsglue.utils import getresolvedOptions

args = getresolvedOptions(sys.argv, ['BUCKET_NAME', 'SCHEMA'])
bucket = args['BUCKET_NAME']
schema = args['SCHEMA']

s3_client = boto3.client('s3')
utc = pytz.utc
_today = datetime.now().replace(tzinfo=utc)
_date = str(_today)[:10]
repot_location = f"source/data/Metadata/History_Deletes/{_date}/DeletedFiles.json"
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def get_key_info(bucket, prefix):
    """
    works to only get us key/file information
    """
    key_names =[]
    file_timestamp = []
    file_size = []
    kwargs = {"bucket": bucket, "prefix": prefix}
    while true:
        try:
            response = s3_client.list_objects_v2(**kwargs)
            for obj i response["Contents"]:
                key_names.append(obj["key"])
                file_timestamp.append(obj["LastModified"].replace(tzinfo=utc))
                file_size.append(obj["Size"])
        except KeyError as e :
            if str(e) == "'Contents'"
            pass
            print("no data in given path")
        try:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break

    key_info = {
        "key_path": key_names,
        "timestamp": file_timestamp,
        "size": file_size
    }
    print(f'All Keys in {bucket} with {prefix} Prefix found!')

    return key_info

def process(s3_file, table_details, deletedFiles):
    """
    method to check expiration of foldes.
    """
    try:
        for i, key_date in enumerate(s3_file["timestamp"]):
            _expire_date = _today- timedelta(days=table_details['expiration_days'])
            if key_date < _expire_date:
                file_path = s3_file["key_path"][i]
                s3_client.delete_object(Bucket = bucket, key = file_path)
                table, partition = file_path.split('/')[5:7]
                if "$" not in partition:
                    if table in deletedFiles:
                        if partition not in deletedFiles[table]:
                            deletedFiles[table].append(partition)
                    else:
                        deletedFiles[table] = [partition]
        return deletedFiles
    except:
        print(f"History data purge failed for {table_details['table_name'].lower()} table:", {sys.exc_info()[1]})

def _history_purge():
    """
    method to check conf file and iterate through all tables to check expiration.
    """
    response = s3_client.get_object(Bucket= 'saikumar71601', key ='purge_config.json')
    json_string = response['Body'].read().decode('utf-8')
    data = json_string.splitlines()
    table_details = {}
    deleted_files = {'Date': _date}
    for i in data:
        table_dict = json.loads(i)
        table_details = table_dict
        table_name = table_details['table_name'].lower()
        prefix =  schema + table_name + "/"
        s3_file = get_key_info(bucket,prefix)
        deleted_partitions = process(s3_file, table_name,deleted_files)
    print(deleted_partitions)
    deleted_partitions = json.dumps(deleted_partitions, ensure_ascii= False, indent= 0, separators=(',',': '))
    s3_client.put_object(Body=deleted_partitions,bucket=bucket,key=repot_location)

_history_purge()

