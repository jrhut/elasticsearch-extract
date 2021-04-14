#!/usr/bin/env python3
"""This is the docstring for the library
TODO:
  * Discuss the use of ENV VARS, most secure way is to use them even if from a library so the secrets never appear in any notebooks.
  * BUT it is hard if you HAVE to use ENV VARS, so we make the methods that need the secrets take them as parameters,
  but have anotehr method that reads the env vars and invokes... encapsulation etc...

  * Also look at writing out JSON - CSV is awesome, but here already

  * Think about what the end goal of this is as a library which is wrapped in Julia.
  * The Julia library has two extra helper methods (at least) one to write the dataframe to file, and one to read from file and return a dataframe
  * NOTE: The wrapper should convert the Pandas or whatever DF into a Julia DF...
  * These methods should/could optionally choose the on disk file format, with a silent default, i.e CSV.
  * This python library:
    * Julia wraps some python function which: takes a query (search terms, filters, etc etc) and returns a dataframe
    * This reads environment variables etc. No need for outputt paths or anything

"""
import argparse
import csv
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import os
import pandas
from dataclasses import dataclass, field


@dataclass
class Query:
    json: str
    index: str = "ps_tweets*"
    out_fields: list = field(default_factory=list)
    paging_id_field: str = "id"
    paging_time_field: str = "created_at"

    def __post_init__(self):
        self.out_fields.append(self.paging_id_field)
        self.out_fields.append(self.paging_time_field)


def _get_env_variables():
    load_dotenv()  # Load variables from .env into system environment
    host = os.getenv("ELASTIC_HOST")
    port = os.getenv("ELASTIC_PORT")
    username = os.getenv("ELASTIC_USER")
    password = os.getenv("ELASTIC_SECRET")

    return host, port, username, password


def _generate_query_json(search_fields:list, search_string:str, field_to_exist:str = None, date_field:str = "created_at", start_date:str = None, end_date:str = None, is_match_all:bool = False):
    json = {"query": {'bool': {}}}

    if start_date != None and end_date != None:
        json['query']['bool']['filter'] = {}
        json['query']['bool']['filter']['range'] = {date_field: {"gte": start_date, "lte": end_date, "format": "yyyy-MM-dd"}}

    not_composite_query = (search_string == None and field_to_exist != None) or (search_string!= None and field_to_exist == None)

    if not_composite_query:
        json['query']['bool']['must'] = {}
        if search_string != None:
            json['query']['bool']['must'] = {"query_string": {"query": search_string, "fields": search_fields}}
        elif field_to_exist != None:
            json['query']['bool']['must'] = {"exists": {"field": field_to_exist}}
    elif is_match_all:
        json['query']['bool']['must'] = {"match_all": {}}
    else:
        json['query']['bool']['must'] = []
        json['query']['bool']['must'].append({"query_string": {"query": search_string, "fields": search_fields}})
        json['query']['bool']['must'].append({"exists": {"field": field_to_exist}})

    return json


def _args_to_query(args):
    json = None
    if args.search != None:
        json = _generate_query_json(args.search[0].split(), args.search[1], args.exists, args.date_field, args.start, args.end, args.match_all)
    else:
        json = _generate_query_json(None, None, args.exists, args.date_field, args.start, args.end, args.match_all)

    fields = []  

    if args.fields:
        fields = args.fields.split()

    query = None

    if args.index:
        query = Query(json=json, out_fields=fields, index=args.index)
    else:
        query = Query(json=json, out_fields=fields)

    return query


def _get_arguments():
    parser = argparse.ArgumentParser("Elasticsearch query tool")
    parser.add_argument("-m", "--match_all", help="Match all fields, downloads all data", action="store_true", default=False)
    parser.add_argument("-s", "--search", help="Takes 2 arguments, fields you want to search then the string query "
                                               "you wish to run",
                        nargs=2,
                        metavar="string",
                        default=None)
    parser.add_argument("-e", "--exists", help="Takes 1 argument, field you want to check for a value",
                        metavar="field", default=None)
    parser.add_argument("-i", "--index", help="Takes 1 argument, index name", metavar="index", default=None)
    parser.add_argument("-f", "--fields", help="Select output fields",
                        metavar="fields", default=None)
    parser.add_argument("-d", "--date_field", help="The date field, default to created_at", default="created_at",
                        metavar="field")
    parser.add_argument("-sd", "--start", help="Starting date to search from yyyy-mm-dd",
                        metavar="date (yyyy-mm-dd)", default=None)
    parser.add_argument("-ed", "--end", help="Ending date to stop searching yyyy-mm-dd or now",
                        metavar="date (yyyy-mm-dd)", default=None)
    parser.add_argument("-o", "--out", help="Output file", default="output.csv")
    args = parser.parse_args()

    return args


def _write_csv_headers(headers:list, out_file:str):  # Writes headers to new csv
    print(f"Writing headers to {out_file}")
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)


def _process_response(hits:list, id_field:str, time_field:str):  # Take list of search results and return the field information and pagination markers
    timestamp = None
    _id = None
    docs = []

    for num, doc in enumerate(hits):
        source_data = doc["_source"]  # Extract the field information
        _id = source_data[id_field]
        timestamp = source_data[time_field]
        docs.append(source_data)  # Add the field JSON information to a document list

    return docs, timestamp, _id


def _process_json(sources:list, fields:list):
    parsed_sources = []
    for source in sources:
        out = {}
        for field in fields:
            if '.' in field:
                f_split = field.split('.')
                try:
                    source[f_split[0]]
                except:
                    out[field] = ""
                    continue
                data = source[f_split[0]]
                for layer in f_split[1:]:
                    if type(data) is list:
                        out[field] = []
                        for e in data:
                            out[field].append(e[layer])
                    else:
                        data = data[layer]
                        out[field] = data
            else:
                try:
                    source[field]
                except:
                    out[field] = ""
                    continue
                out[field] = source[field]
        parsed_sources.append(out)

    return parsed_sources


def _query_to_csv_large(host:str, port:str, username:str, password:str, query:Query, out_file:str):
    es = Elasticsearch([host], http_auth=(username, password), scheme="https", port=port,
                    verify_certs=False, ssl_show_warn=False)  # Open connection to the Elasticsearch database

    print("Counting documents in query")

    response = es.count(index=query.index, body=query.json)  # Send a count query to check the total hits of the search
    document_count = response['count']

    print(f"Found {document_count} documents matching query")
    print("Beginning download")

    current_count = 0

    _write_csv_headers(query.out_fields, out_file)

    with open(out_file, "a", newline="", encoding='utf-8') as file:
        writer = csv.DictWriter(file, query.out_fields)

        while True:  # Main response loop
        
            # Search query on main index using max documents per query (10,000) and sort to allow for paging
            
            response = es.search(index=query.index, size=10000,
                                sort=[f"{query.paging_time_field}:asc", f"{query.paging_id_field}:asc"],
                                body=query.json, _source=query.out_fields)
            
            res_docs = response["hits"]["hits"]

            if not res_docs:  # If no new responses returned leave loop
                break

            current_count += len(res_docs)
            print(f"Downloading: [{current_count}/{document_count}]")

            elastic_docs, last_timestamp, last_id = _process_response(res_docs, query.paging_id_field, query.paging_time_field)  # Extracts data from nested JSON

            query.json["search_after"] = [last_timestamp, last_id]  # Set page marker to last result

            rows = _process_json(elastic_docs, query.out_fields)
            writer.writerows(rows)

    if current_count > 0:
        print("Saved data to query_output.csv")
    else:
        print("No results nothing saved")

    print("Done")


def _query_to_dataframe(host:str, port:str, username:str, password:str, query:Query) -> Query:
    """ This is the base function that takes elasticsearch connection arguments and a query object
    and returns a pandas dataframe
    """

    print("Connecting to elasticsearch")

    es = Elasticsearch([host], http_auth=(username, password), scheme="https", port=port,
                    verify_certs=False, ssl_show_warn=False)  # Open connection to the Elasticsearch database

    print("Counting documents in query")

    response = es.count(index=query.index, body=query.json)  # Send a count query to check the total hits of the search
    document_count = response['count']

    print(f"Found {document_count} documents matching query")
    print("Beginning download")

    current_count = 0

    rows = []

    while True:  # Main response loop

        # Search query on main index using max documents per query (10,000) and sort to allow for paging
        response = es.search(index=query.index, size=10000,
                            sort=[f"{query.paging_time_field}:asc", f"{query.paging_id_field}:asc"],
                            body=query.json, _source=query.out_fields)

        res_docs = response["hits"]["hits"]

        if not res_docs:  # If no new responses returned leave loop
            break

        current_count += len(res_docs)
        print(f"Downloading: [{current_count}/{document_count}]")

        elastic_docs, last_timestamp, last_id = _process_response(res_docs, query.paging_id_field, query.paging_time_field)  # Extracts data from nested JSON

        query.json["search_after"] = [last_timestamp, last_id]  # Set page marker to last result

        rows += _process_json(elastic_docs, query.out_fields)

    df = pandas.DataFrame(rows, columns=query.out_fields)
    print("Done")

    return df


def _query_to_json(host:str, port:str, username:str, password:str, query:Query) -> Query:
    print("Connecting to elasticsearch")

    es = Elasticsearch([host], http_auth=(username, password), scheme="https", port=port,
                    verify_certs=False, ssl_show_warn=False)  # Open connection to the Elasticsearch database

    print("Counting documents in query")

    response = es.count(index=query.index, body=query.json)  # Send a count query to check the total hits of the search
    document_count = response['count']

    print(f"Found {document_count} documents matching query")
    print("Beginning download")

    current_count = 0

    rows = []

    while True:  # Main response loop

        # Search query on main index using max documents per query (10,000) and sort to allow for paging
        response = es.search(index=query.index, size=10000,
                            sort=[f"{query.paging_time_field}:asc", f"{query.paging_id_field}:asc"],
                            body=query.json, _source=query.out_fields)

        res_docs = response["hits"]["hits"]

        if not res_docs:  # If no new responses returned leave loop
            break

        current_count += len(res_docs)
        print(f"Downloading: [{current_count}/{document_count}]")

        elastic_docs, last_timestamp, last_id = _process_response(res_docs, query.paging_id_field, query.paging_time_field)  # Extracts data from nested JSON

        query.json["search_after"] = [last_timestamp, last_id]  # Set page marker to last result

        rows += elastic_docs

    print("Done")

    return rows


def query_to_dataframe(index:str = None, return_fields:list = [], fields_to_search:list = [], search_string:str = None, field_to_exist:str = None, date_field:str = "created_at", start_date:str = None, end_date:str = None, is_match_all:bool = False):
    host, port, username, password = _get_env_variables()

    json = _generate_query_json(fields_to_search, search_string, field_to_exist, date_field, start_date, end_date, is_match_all)

    query = None

    if index == None:
        query = Query(json, out_fields=return_fields)
    else:
        query = Query(json, index=index, out_fields=return_fields)

    print(query.json)

    return _query_to_dataframe(host, port, username, password, query)


def query_to_json(index:str = None, return_fields:list = [], fields_to_search:list = [], search_string:str = None, field_to_exist:str = None, date_field:str = "created_at", start_date:str = None, end_date:str = None, is_match_all:bool = False):
    host, port, username, password = _get_env_variables()

    json = _generate_query_json(fields_to_search, search_string, field_to_exist, date_field, start_date, end_date, is_match_all)

    query = None

    if index == None:
        query = Query(json, out_fields=return_fields)
    else:
        query = Query(json, index=index, out_fields=return_fields)

    print(query.json)

    return _query_to_json(host, port, username, password, query)


def write_dataframe_to_file(df, path, format):
    pass


def main():
    """This is the main function which reads the env vars (DISCUSS) and parsees command line args.
    Look up 'arg pass'
    1. Read arguments and fail
    2. Do checks on FS etc.
    3. Then do stuff...
    """

    host, port, username, password = _get_env_variables()

    args = _get_arguments()
    output_file = args.out
    query = _args_to_query(args)
    print(query.json)

    #print(_query_to_dataframe(host, port, username, password, query))
    _query_to_csv_large(host, port, username, password, query, output_file)
    #print(query_to_dataframe(fields_to_search=['full_text'], search_string='vaccine', field_to_exist='entities.urls.expanded_url'))
    #print(query_to_json(fields_to_search=['full_text'], search_string='vaccine', field_to_exist='entities.urls.expanded_url'))
    

if __name__ == '__main__':
    main()
