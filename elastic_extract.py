from elasticsearch import Elasticsearch
import pandas
import os
import csv
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env into system environment
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_PORT = os.getenv("ELASTIC_PORT")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_SECRET = os.getenv("ELASTIC_SECRET")

QUERY = {
    "query": {
        "match_all": {}
    }
}

FILENAME = "query_output.csv"  # Output filename

INDEX = "ps_tweets*"
PAGING_ID_FIELD = "id"  # Name of a unique identifying id field
PAGING_TIMESTAMP_FIELD = "created_at"  # Name of some kind of datetime field


def process_response(hits):  # Take list of search results and return the field information and pagination markers
    timestamp = None
    _id = None
    docs = []

    for num, doc in enumerate(hits):
        source_data = doc["_source"]  # Extract the field information
        _id = source_data[PAGING_ID_FIELD]
        timestamp = source_data[PAGING_TIMESTAMP_FIELD]
        docs.append(source_data)  # Add the field JSON information to a document list

    return docs, timestamp, _id


def write_csv_headers(frame):  # Writes headers to new csv
    print(f"Writing headers to {FILENAME}")
    with open(FILENAME, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(frame.columns)


es = Elasticsearch([ELASTIC_HOST], http_auth=(ELASTIC_USER, ELASTIC_SECRET), scheme="https", port=ELASTIC_PORT,
                   verify_certs=False, ssl_show_warn=False) # Open connection to the Elasticsearch database

print("Counting documents in query")

response = es.count(index=INDEX, body=QUERY)  # Send a count query to check the total hits of the search
document_count = response['count']

print(f"Found {document_count} documents matching query")
print("Beginning download")

current_count = 0
headers = []
df = pandas.DataFrame()

while True:  # Main response loop
    # Search query on main index using max documents per query (10,000) and sort to allow for paging
    response = es.search(index=INDEX, size=10000, sort=[f"{PAGING_TIMESTAMP_FIELD}:asc", f"{PAGING_ID_FIELD}:asc"], body=QUERY)
    res_docs = response["hits"]["hits"]

    if not res_docs:  # If no new responses returned leave loop
        break

    current_count += len(res_docs)
    print(f"Downloading: [{current_count}/{document_count}]")

    elastic_docs, last_timestamp, last_id = process_response(res_docs)  # Extracts data from nested JSON

    QUERY["search_after"] = [last_timestamp, last_id]  # Set page marker to last result

    if df.size == 0:  # First iteration
        df = pandas.DataFrame(elastic_docs)  # Create a dataframe to convert JSON to table
        headers = df.columns
        write_csv_headers(df)  # Create csv
        continue  # Skip the append to avoid duplicate data

    if len(df.index) >= 100000:  # When the dataframe becomes too large save it to the csv and empty it
        print("Saving large data chunk")
        df.to_csv(FILENAME, ",", mode="a", header=False, index=False)  # Appending to output csv
        df = pandas.DataFrame(columns=headers)  # Clear dataframe

    df = df.append(elastic_docs)  # Add new documents to the dataframe

if df.size > 0:  # If results are left in dataframe after exiting main loop
    df.to_csv(FILENAME, ",", mode="a", header=False, index=False)  # Append them to the csv

if current_count > 0:
    print("Saved data to query_output.csv")
else:
    print("No results nothing saved")

print("Done")
