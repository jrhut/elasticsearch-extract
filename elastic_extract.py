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

  * Add JSON save to file
  *

"""
import argparse
import csv
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import os
import pandas
from dataclasses import dataclass, field
import json


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


def _get_env_variables() -> (str, str, str, str):
    """ Load and return the environment variables.

    Returns:
        str: env var 'ELASTIC_HOST'
        str: env var 'ELASTIC_PORT'
        str: env var 'ELASTIC_USER'
        str: env var 'ELASTIC_SECRET'
    """
    load_dotenv()  # Load variables from .env file into system environment
    host = os.getenv("ELASTIC_HOST")
    port = os.getenv("ELASTIC_PORT")
    username = os.getenv("ELASTIC_USER")
    password = os.getenv("ELASTIC_SECRET")

    return host, port, username, password


def _generate_query_json(search_fields:list, search_string:str, field_to_exist:str = None, date_field:str = "created_at", start_date:str = None, end_date:str = None, is_match_all:bool = False) -> dict:
    """ Generates the query json body from the simple query parameters.

    Args:
        search_fields (list): the fields you want to search for your query string in
        search_string (str): the terms you want to search for in the search fields
        field_to_exist (str): supplied field will be used as an extra check to 
            only return documents where this field isn't null
        date_field (str): supplied field will be used to search by a custom date field
            use in conjunction with start_date and end_date args
        start_date (str): the first date you want to return documents from in format
            yyyy-mm-dd
        end_date (str): the last date you want to return documents from in format
            yyyy-mm-dd, can also be set to 'now' to use current date
        is_match_all (bool): this overrides search terms and exist terms and returns
            all documents between start and end dates if specified

    Returns:
        dict: the json query body used to make API calls
    """
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


def _args_to_query(args:argparse.ArgumentParser) -> Query:
    """ Take the arguments and return a Query object.

    Args:
        args (argparse.ArgumentParser): the arguments passed to the program

    Returns:
        Query: the query object used to call API functions
    """
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


def _check_arguments(args:argparse.ArgumentParser) -> bool:
    """ Check that arguments are valid and fail early if not.

    Args:
        args (argparse.ArgumentParser): the arguments passed to the program

    Returns:
        bool: True if passes argument checks, False if fails
    """
    if  not args.search and not args.exists and not args.match_all:
        print("Eerror: you must provide a search term")
        return False
    elif (args.start and not args.end) or (not args.start and args.end):
        print("error: you must provide both a start and an end date")
        return False
    elif not os.path.exists(os.path.dirname(args.out)) and os.path.dirname(args.out) != '':
        print(f"error: the directory '{os.path.dirname(args.out)}' does not exist.")
        return False

    return True


def _get_arguments() -> argparse.ArgumentParser:
    """ Parse the command line arguments and return them as an argument
    object.

    Returns:
        argparse.ArgumentParser: the arguments passed to the program
    """
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
    parser.add_argument("-o", "--out", help="Output file with path", default="output.csv")
    args = parser.parse_args()

    return args


def _write_csv_headers(headers:list, out_file:str) -> None:
    """ Write the return fields to the csv file as headers

    Args:
        headers (list): list of DataFrame column headers (all return fields)
        out_file (str): the path including filename of the file to write to
    """
    print(f"Writing headers to {out_file}")
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)


def _get_docs_from_response(hits:list, id_field:str, time_field:str) -> (list, str, str):
    """ Extract the actual document data from the raw response and extract
    pagination information.

    Args:
        hits (list): list of raw response json objects from elasticsearch
        id_field (str): the id pagination field to update
        time_field (str): the time pagination field to update

    Returns:
        list: a list of document data extracted from raw responses
        str: the latest timestamp to update pagination
        str: the latest id to update pagination
    """
    timestamp = None
    _id = None
    docs = []

    for num, doc in enumerate(hits):
        source_data = doc["_source"]  # Extract the field information
        _id = source_data[id_field]
        timestamp = source_data[time_field]
        docs.append(source_data)  # Add the field JSON information to a document list

    return docs, timestamp, _id


def _clean_elastic_docs(sources:list, fields:list) -> list:
    """ Clean the document json data from elasticsearch. This cleaning
    includes inserting empty strings for nulls and unnesting
    nested fields.

    Args:
        sources (list): list of document data (from _source object) extracted from raw responses
        fields (list): list of fields in the data

    Returns:
        list: a list of documents that have been cleaned for export
    """
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


def _query_to_csv_large(host:str, port:str, username:str, password:str, query:Query, out_file:str) -> None:
    """ This is the internal function for handling the connection to elasticsearch and
    the subsequent API calls with the data provided from the Query object. It then saves
    the results to a csv file.

    Args:
        host (str): the elasticsearch host address
        port (str): the elasticsearch port
        username (str): the elasticsearch username login
        password (str): the elasticsearch password
        query (Query): query object containing desired query
    """
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
            elastic_docs, last_timestamp, last_id = _get_docs_from_response(res_docs, query.paging_id_field, query.paging_time_field)  # Extracts data from nested JSON
            query.json["search_after"] = [last_timestamp, last_id]  # Set page marker to last result
            rows = _clean_elastic_docs(elastic_docs, query.out_fields)
            writer.writerows(rows)

    if current_count > 0:
        print("Saved data to query_output.csv")
    else:
        print("No results nothing saved")

    print("Done")


def _query_to_json(host:str, port:str, username:str, password:str, query:Query) -> Query:
    """ This is the internal function for handling the connection to elasticsearch and
    the subsequent API calls with the data provided from the Query object. It then returns
    the resultts in a list of JSON objects.

    Args:
        host (str): the elasticsearch host address
        port (str): the elasticsearch port
        username (str): the elasticsearch username login
        password (str): the elasticsearch password
        query (Query): query object containing desired query

    Returns:
        list: a list of cleaned json documents returned by the query
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
        elastic_docs, last_timestamp, last_id = _get_docs_from_response(res_docs, query.paging_id_field, query.paging_time_field)  # Extracts data from nested JSON
        query.json["search_after"] = [last_timestamp, last_id]  # Set page marker to last result
        rows += _clean_elastic_docs(elastic_docs, query.out_fields)

    print("Done")
    return rows


def query_to_json(index:str = None, return_fields:list = [], fields_to_search:list = [], search_string:str = None, field_to_exist:str = None, date_field:str = "created_at", start_date:str = None, end_date:str = None, is_match_all:bool = False) -> list:
    """ This is the function that takes in query parameters and returns a list of json objects from
    elasticsearch documents. This function creates a query object and calls an internal function that handles
    the actual communication with elasticsearch.

    Args:
        index (str): the elasticsearch index you want to query
        return_fields (list): the fields you want returned from the query
        fields_to_search (list): the fields you want to search for your query string in
        search_string (str): the terms you want to search for in the search fields
        field_to_exist (str): supplied field will be used as an extra check to 
            only return documents where this field isn't null
        date_field (str): supplied field will be used to search by a custom date field
            use in conjunction with start_date and end_date args
        start_date (str): the first date you want to return documents from in format
            yyyy-mm-dd
        end_date (str): the last date you want to return documents from in format
            yyyy-mm-dd, can also be set to 'now' to use current date
        is_match_all (bool): this overrides search terms and exist terms and returns
            all documents between start and end dates if specified

    Returns:
        list: a list of cleaned json documents returned by the query
    """
    host, port, username, password = _get_env_variables()

    json = _generate_query_json(fields_to_search, search_string, field_to_exist, date_field, start_date, end_date, is_match_all)

    query = None
    if index == None:
        query = Query(json, out_fields=return_fields)
    else:
        query = Query(json, index=index, out_fields=return_fields)

    response_list = _query_to_json(host, port, username, password, query)
    response_json = {"data":response_list}

    return response_json


def query_to_dataframe(index:str = None, return_fields:list = [], fields_to_search:list = [], search_string:str = None, field_to_exist:str = None, date_field:str = "created_at", start_date:str = None, end_date:str = None, is_match_all:bool = False) -> pandas.DataFrame:
    """ This is the function that takes in query parameters and returns a pandas datafram from
    elasticsearch documents. This function creates a query object and calls an internal function that handles
    the actual communication with elasticsearch.

    Args:
        index (str): the elasticsearch index you want to query
        return_fields (list): the fields you want returned from the query
        fields_to_search (list): the fields you want to search for your query string in
        search_string (str): the terms you want to search for in the search fields
        field_to_exist (str): supplied field will be used as an extra check to 
            only return documents where this field isn't null
        date_field (str): supplied field will be used to search by a custom date field
            use in conjunction with start_date and end_date args
        start_date (str): the first date you want to return documents from in format
            yyyy-mm-dd
        end_date (str): the last date you want to return documents from in format
            yyyy-mm-dd, can also be set to 'now' to use current date
        is_match_all (bool): this overrides search terms and exist terms and returns
            all documents between start and end dates if specified

    Returns:
        pandas.DataFrame: a DataFrame where the json fields are columns
    """
    response_json = query_to_json(index, return_fields, fields_to_search, search_string, field_to_exist, date_field, start_date, end_date, is_match_all)  
    df = pandas.DataFrame()
    if len(response_json['data']) > 0:
        fields = response_json['data'][0].keys()
        df = pandas.DataFrame(response_json['data'], columns=fields)
    return df


def write_dataframe_to_file(df:pandas.DataFrame, path:str, format:str="csv") -> None:
    """ This function takes a dataframe and exports it to either JSON or CSV.
    NOTE: This function could be put in the Julia wrapper?

    Args:
        df (pandas.DataFrame): the dataframe to be stored to file
        path (str): the path including filename for the output
        format (str): the format of the file on disk either 'json' 
            or 'csv'
    """
    if format == "json":
        df_json = df.to_json(orient="records")

        with open(path, 'w') as f:
            json.dump(df_json, f)

    elif format == "csv":
        df.to_csv(path, index=False)


def read_dataframe_from_file(df:pandas.DataFrame, path:str) -> None:
    """ Function to read in either a json formated file or csv into a dataframe.
    NOTE: This function could be put in the Julia wrapper?

    Args:
        df (pandas.DataFrame): the dataframe to be stored to file
        path (str): the path including filename for the output
    """
    df = pandas.read_csv(path)
    return df


def main() -> None:
    """This is the main function which reads the env vars (DISCUSS) and parsees command line args.
    Look up 'arg pass'
    1. Read arguments and fail
    2. Do checks on FS etc.
    3. Then do stuff...
    """

    host, port, username, password = _get_env_variables()
    args = _get_arguments()
    
    if not _check_arguments(args):
        print('Done')
        return
   
    output_file = args.out
    query = _args_to_query(args)
    _query_to_csv_large(host, port, username, password, query, output_file)

    # Testing external functions here
    
    #print(query_to_dataframe(fields_to_search=['full_text'], search_string='vaccine', field_to_exist='entities.urls.expanded_url'))
    
    #print(query_to_json(fields_to_search=['full_text'], search_string='vaccine', field_to_exist='entities.urls.expanded_url'))

    #df = query_to_dataframe(fields_to_search=['full_text'], search_string='vac', field_to_exist='entities.urls.expanded_url')
    #write_dataframe_to_file(df, 'anything.json', 'json')
    

if __name__ == '__main__':
    main()