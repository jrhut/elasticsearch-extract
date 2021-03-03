# Elasticsearch Query Extractor

Extracts all query results from Elasticsearch and saves it to query_output.csv!

# Getting Started

You will want to create a .env file in the repo root, containing the variables:\
ELASTIC_HOST\
ELASTIC_PORT\
ELASTIC_USER\
ELASTIC_SECRET
  
Change the query using the QUERY variable in the main script, the default is a match_all to gather all data in the index (WARNING: 3.3M results). 


Find help on Elasticsearch documentation here https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html.  
You can also test your query in the kibana dev tools sandbox. (very handy)

# Query Examples

Default query to gather entire index

```
QUERY = {
    "query": {
        "match_all": {}
    }
}
```


Wildcard string query on multiple fields

```
QUERY = {
    "query": {
        "query_string": {"query": "vac* OR vax*", "fields": ["full_text", "quoted_status.full_text"]}
    }
}
```

Boolean searches on different fields using must (AND) and should (OR)

```
QUERY = {
    "query": {
        "bool": {
            "must": [
                {
                    "query_string": {"query": "vac* OR vax*", "default_field": "full_text"}
                },
                {
                    "query_string": {"query": "vac* OR vax*", "default_field": "full_text"}
                }
            ]
        }
    }
}
```

```
QUERY = {
    "query": {
        "bool": {
            "should": [
                {
                    "query_string": {"query": "vac* OR vax*", "default_field": "full_text"}
                },
                {
                    "term": {"is_quote_status": True}
                }
            ]
        }
    }
}
```
