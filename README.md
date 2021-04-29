## Esextract

Makes extracting queries from elasticseach a little bit easier.

## Installation

Requires python3.8

Install the python package esextract ```pip install esextract```

Now configue some environment variables as so

```
ELASTIC_HOST = {{ELASTICSEARCH HOST ADDRESS}}
ELASTIC_PORT = {{ELASTICSEARCH PORT}}
ELASTIC_USER = {{ELASTICSEARCH LOGIN USERNAME}}
ELASTIC_SECRET = {{ELASTICSEARCH LOGIN PASSWORD}}
```

Optionally (reccomended) to make life easier also configure the default variables below. This will save you needing extra arguments when making a query.

```
DEFAULT_INDEX = {{DEFAULT INDEX TO QUERY}}
DEFAULT_DATE_FIELD = {{DEFAULT DATE FIELD FOR RANGE SEARCHES}} 
PAGE_ID_FIELD = {{THE DOCUMENT ID FIELD TO PAGE ON}}
PAGE_TIME_FIELD = {{THE DOCUMENT DATE FIELD TO PAGE ON}}
```

# Command Line Usage

When the module is run in terminal it will take some query parameters and output a csv containing the query response.

NOTE: You must have atleast one query term, --search, --exists or --match_all.

```
python -m esextract --help
```

The most basic query is a preset called --match_all that gathers all tweets and returns the fields chosen

```
python -m esextract --match_all --fields "{FIELD} {FIELD} ..."
```

You can combine this with the --start and --end to get all tweets in some date rangethe date format used is YYYY-MM-DD. For end date you can also use "now" to use the current date.

```
python -m esextract --match_all --fields "{FIELD} ..." --start "{START DATE}" --end "{END DATE}"
```

The general format of a string search looks something like this

```
python -m esextract --search "{FIELD}" "{SEARCH TERMS}" --fields "{FIELD} ..." --start "{START DATE}" --end "{END DATE}"
```

The general format of an exists search looks like this

```
python -m esextract --exists "{FIELD}" --fields "{FIELD} ..." --start "{START DATE}" --end "{END DATE}"
```

You can combine them like so 

```
python -m esextract --search "{FIELD}" "{SEARCH TERMS}" --exists "{FIELD}" --fields "{FIELD} ..."
```

# Examples

```
python elastic_extract.py --match_all --fields "user.id conversation_id entities.urls.expanded_url" --start "2020-01-01" --end "now"
```

```
python elastic_extract.py --search "full_text" "vac* OR vax*" --exists "entities.urls.expanded_url" --fields "user.id full_text" --start "2020-09-13" --end "now"
```

# Import Usage

When imported the module povides access to four functions: query_to_dataframe, query_to_json, write_dataframe_to_file, read_dataframe_from_file.

```
    query_to_dataframe(index, paging_id_field, paging_time_field, return_fields, fields_to_search, search_string, field_to_exist, date_field, start_date, end_date, is_match_all)

""" This is the function that takes in query parameters and returns a pandas datafram from
elasticsearch documents. This function creates a query object and calls an internal function that handles
the actual communication with elasticsearch.
Args:
    index (str): the elasticsearch index you want to query
    paging_id_field (str): the id field to page on
    paging_time_field (str): the date/time field to page on
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
```

```
    query_to_json(index, paging_id_field, paging_time_field, return_fields, fields_to_search, search_string, field_to_exist, date_field, start_date, end_date, is_match_all)

""" This is the function that takes in query parameters and returns a list of json objects from
elasticsearch documents. This function creates a query object and calls an internal function that handles
the actual communication with elasticsearch.
Args:
    index (str): the elasticsearch index you want to query
    paging_id_field (str): the id field to page on
    paging_time_field (str): the date/time field to page on
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
```

```
    write_dataframe_to_file(df, path, format)
    
""" This function takes a dataframe and exports it to either JSON, CSV or Arrow Parquet.
NOTE: This function could be put in the Julia wrapper?
Args:
    df (pandas.DataFrame): the dataframe to be stored to file
    path (str): the path including filename for the output
    format (str): the format of the file on disk (csv, json, arrow)
"""
```

```
    read_dataframe_from_file(path)
""" Function to read in csv, json or arrow parquet to a dataframe.
NOTE: This function could be put in the Julia wrapper?
Args:
    path (str): the path including filename for the output
Returns:
    pandas.DataFrame: the csv in a DataFrame
"""
```

# Examples

```
# Without optional environment variables 
query_to_dataframe(index="index01", paging_id_field="id", paging_time_field="date", fields_to_search=["body"], search_string="Hello!")
query_to_json(index="index01", paging_id_field="id", paging_time_field="date", is_match_all=True, date_field="created_at", start_date="2020-01-01", end_date="2021-01-01")

# With optional environment variables
query_to_json(field_to_exist="url", fields_to_search=["body"], search_string="Bye!")
query_to_json(is_match_all=True, start_date="2020-01-01", end_date="2021-01-01")
```
