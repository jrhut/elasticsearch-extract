from elasticsearch import Elasticsearch
import pandas
import os
from dotenv import load_dotenv

load_dotenv()
KIBANA_HOST = os.getenv("KIBANA_HOST")
KIBANA_PORT = os.getenv("KIBANA_PORT")
KIBANA_USER = os.getenv("KIBANA_USER")
KIBANA_SECRET = os.getenv("KIBANA_SECRET")

QUERY = {
    "query": {
        "match_all": {}
    }
}


def get_last_doc(docs, hits):
    timestamp = None
    _id = None

    for num, doc in enumerate(hits):
        source_data = doc["_source"]
        _id = source_data["id"]
        timestamp = source_data["created_at"]
        docs.append(source_data)

    return docs, timestamp, _id


es = Elasticsearch([KIBANA_HOST], http_auth=(KIBANA_USER, KIBANA_SECRET), scheme="https", port=KIBANA_PORT,
                   verify_certs=False, ssl_show_warn=False)

elastic_docs = []

print("Counting documents in query")

response = es.count(index="ps_tweets*", body=QUERY)
document_count = response['count']

print(f"Found {document_count} documents matching query")
print("Beginning download")

while True:
    response = es.search(index="ps_tweets*", size=10000, sort=["created_at:asc", "id:asc"], body=QUERY)
    res_docs = response["hits"]["hits"]

    if not res_docs:
        break

    elastic_docs, last_timestamp, last_id = get_last_doc(elastic_docs, res_docs)
    QUERY["search_after"] = [last_timestamp, last_id]

    print(f"Download: [{len(elastic_docs)}/{document_count}]")

print("Saving data to query_output.csv")
df = pandas.DataFrame(elastic_docs)
df.to_csv("query_output.csv", ",")
print("Done")
