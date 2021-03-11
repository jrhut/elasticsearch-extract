from elasticsearch import Elasticsearch
import pandas
import os
import csv
from dotenv import load_dotenv
import argparse

load_dotenv()  # Load variables from .env into system environment
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_PORT = os.getenv("ELASTIC_PORT")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_SECRET = os.getenv("ELASTIC_SECRET")

FILENAME = "query_output.csv"  # Output filename
INDEX = "ps_tweets*"
PAGING_ID_FIELD = "id"  # Name of a unique identifying id field
PAGING_TIMESTAMP_FIELD = "created_at"  # Name of some kind of datetime field
QUERY = {"query": {}}
FIELDS = []


def bool_parser(q, b, a):
    if (a.search and not a.exists) or (a.exists and not a.search):
        q['query']['bool'][b] = {}
        if a.search:
            q['query']['bool'][b] = {"query_string": {"query": a.search[0][1],
                                                      "fields": a.search[0][0].split()}}
        elif a.exists:
            q['query']['bool'][b] = {"exists": {"field": a.exists[0]}}
    elif a.match_all:
    	q['query']['bool'][b] = {"match_all": {}}
    else:
        q['query']['bool'][b] = []
        for i in range(len(a.search)):
            q['query']['bool'][b].append({"query_string": {"query": a.search[i][1],
                                                           "fields": a.search[i][0].split()}})
        for i in range(len(a.exists)):
            q['query']['bool'][b].append({"exists": {"field": a.exists[i]}})

    print(q)
    return q


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
    with open(FILENAME, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(frame.columns)


def parse_arguments(q, f):
    parser = argparse.ArgumentParser("Elasticsearch query tool")
    parser.add_argument("-m", "--match_all", help="Match all fields, downloads all data", action="store_true")
    parser.add_argument("-s", "--search", help="Takes 2 arguments, fields you want to search then the string query "
                                               "you wish to run",
                        nargs=2,
                        action="append",
                        metavar="string")
    parser.add_argument("-e", "--exists", help="Takes 1 argument, field you want to check for a value", action="append",
                        metavar="field")
    parser.add_argument("-a", "--AND", help="Link multiple queries together", action="store_true")
    parser.add_argument("-o", "--OR", help="Link multiple queries together", action="store_true")
    parser.add_argument("-f", "--fields", help="Select output fields",
                        metavar="fields")
    parser.add_argument("-d", "--date_field", help="The date field, default to created_at", default="created_at",
                        metavar="field")
    parser.add_argument("-sd", "--start", help="Starting date to search from yyyy-mm-dd",
                        metavar="date (yyyy-mm-dd)")
    parser.add_argument("-ed", "--end", help="Ending date to stop searching yyyy-mm-dd or now",
                        metavar="date (yyyy-mm-dd)")
    args = parser.parse_args()

    if args.match_all:
        q = bool_parser(q, "must", args)

    if args.AND or args.OR or args.start:
        q['query']['bool'] = {}

    if args.start and args.end:
        q['query']['bool']['filter'] = {}
        q['query']['bool']['filter']['range'] = {
            args.date_field: {"gte": args.start, "lte": args.end, "format": "yyyy-MM-dd"}}

    if args.AND:
        q = bool_parser(q, "must", args)
    elif args.OR:
        q = bool_parser(q, "should", args)
    else:
        q = bool_parser(q, "must", args)

    if args.fields:
        f = ['id', 'created_at']
        f += args.fields.split()

    return q, f, args


QUERY, FIELDS, arguments = parse_arguments(QUERY, FIELDS)

es = Elasticsearch([ELASTIC_HOST], http_auth=(ELASTIC_USER, ELASTIC_SECRET), scheme="https", port=ELASTIC_PORT,
                   verify_certs=False, ssl_show_warn=False)  # Open connection to the Elasticsearch database

print("Counting documents in query")

print(QUERY)

response = es.count(index=INDEX, body=QUERY)  # Send a count query to check the total hits of the search
document_count = response['count']

print(f"Found {document_count} documents matching query")
print("Beginning download")

current_count = 0
headers = []
df = pandas.DataFrame()

while True:  # Main response loop
    # Search query on main index using max documents per query (10,000) and sort to allow for paging
    if arguments.fields:
        response = es.search(index=INDEX, size=10000, sort=[f"{PAGING_TIMESTAMP_FIELD}:asc", f"{PAGING_ID_FIELD}:asc"],
                             body=QUERY, _source=FIELDS)
    else:
        response = es.search(index=INDEX, size=10000, sort=[f"{PAGING_TIMESTAMP_FIELD}:asc", f"{PAGING_ID_FIELD}:asc"],
                             body=QUERY)
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
