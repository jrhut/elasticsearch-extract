from elasticsearch import Elasticsearch
import pandas
import os
import csv
from dotenv import load_dotenv

load_dotenv()
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_PORT = os.getenv("ELASTIC_PORT")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_SECRET = os.getenv("ELASTIC_SECRET")

QUERY = {
    "query": {
        "match": {"full_text": "vaccine"}
    }
}

FILENAME = "query_output.csv"


def process_response(hits):
    timestamp = None
    _id = None
    docs = []

    for num, doc in enumerate(hits):
        source_data = doc["_source"]
        _id = source_data["id"]
        timestamp = source_data["created_at"]
        docs.append(source_data)

    return docs, timestamp, _id


def write_csv_headers(frame):
    print(f"Writing headers to {FILENAME}")
    with open(FILENAME, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(frame.columns)


es = Elasticsearch([ELASTIC_HOST], http_auth=(ELASTIC_USER, ELASTIC_SECRET), scheme="https", port=ELASTIC_PORT,
                   verify_certs=False, ssl_show_warn=False)

print("Counting documents in query")

response = es.count(index="ps_tweets*", body=QUERY)
document_count = response['count']

print(f"Found {document_count} documents matching query")
print("Beginning download")

count = 0
headers = []
df = None

while True:
    response = es.search(index="ps_tweets*", size=10000, sort=["created_at:asc", "id:asc"], body=QUERY)
    res_docs = response["hits"]["hits"]

    count += len(res_docs)
    print(f"Downloading: [{count}/{document_count}]")

    if not res_docs:
        break

    elastic_docs, last_timestamp, last_id = process_response(res_docs)

    QUERY["search_after"] = [last_timestamp, last_id]

    if df is None:
        df = pandas.DataFrame(elastic_docs)
        headers = df.columns
        write_csv_headers(df)
        continue

    if len(df.index) >= 10000:
        print("Saving large data chunk")
        df.to_csv(FILENAME, ",", mode="a", header=False, index=False)
        df = pandas.DataFrame(columns=headers)

    df = df.append(elastic_docs)


if df.size > 0:
    df.to_csv(FILENAME, ",", mode="a", header=False, index=False)

print("Saved data to query_output.csv")
print("Done")
