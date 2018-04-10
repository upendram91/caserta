import requests
import json
import csv
import os
import google.cloud.storage as storage
import google.cloud.bigquery as bigquery
def fetch_api_data(url):

    r = requests.get(url=url)
    if r.status_code == requests.codes.ok:
        return r.content

def save_json_as_csv(data, file_name, file_path="data"):
    with open(file_path + file_name, 'w') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(data[0].keys())
        for row in data:
            csv_writer.writerow(row.values())

def upload_to_gcp_bucket(source_file_name, bucket_name, file_path = "data/"):

    bucket = storage_client.get_bucket(bucket_name)
    file_to_upload = file_path + source_file_name

    # source_file_name = 'Local file to upload, for example ./file.txt'
    blob = bucket.blob(os.path.basename(source_file_name))

    # Upload the local file to Cloud Storage.
    blob.upload_from_filename(file_to_upload)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        bucket))

def bq_load_data_from_gcs(dataset_id, table_name, bucket_ref):
    """
    creates a new table in bigquery using a csv file as input from google cloud storage

    :param dataset_id:
    :param table_name:
    :param bucket_ref:
    :return:
    """
    dataset_ref = bq_client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = bq_client.load_table_from_uri(
        bucket_ref,
        dataset_ref.table(table_name),
        job_config=job_config)  # API request

    print(load_job.job_type == 'load')
    load_job.result()  # Waits for table load to complete.
    print(load_job.state == 'DONE')
    print(bq_client.get_table(dataset_ref.table(table_name)).num_rows > 0)

def bq_get_data(query):
    QUERY = (query)
    query_job = bigquery.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        print(row.name)

if __name__ == "__main__":
    creds_file = 'conf/caserta-gcpeval-09b57eb04042.json'
    storage_client = storage.Client.from_service_account_json(creds_file)
    bq_client = bigquery.Client.from_service_account_json(creds_file)

    bucket_name = 'crypto-store'
    # data set name
    dataset_name = 'caserta_crypto_store'
    # big query table name
    table_name = "crypto_data"
    # call REST API and fetch data
    url = "https://api.coinmarketcap.com/v1/ticker/?limit=100000"

    data = fetch_api_data(url)
    json_data = json.loads(data)

    # take json input and generate csv file
    source_file_name = "cyrptodata.csv"
    save_json_as_csv(json_data, source_file_name, file_path="data/")
    upload_to_gcp_bucket(source_file_name, bucket_name)

    # create and load automatically from gcs to bq
    bucket_ref="gs://{}/{}".format(bucket_name,source_file_name)
    bq_load_data_from_gcs(dataset_name, table_name, bucket_ref)
