# Elasticsearch Query Extractor

You will want to create a .env file in the repo root, containing the variables:

ELASTIC_HOST\
ELASTIC_PORT\
ELASTIC_USER\
ELASTIC_SECRET
  
Change the query using the QUERY variable in the main script, the default is a match_all to gather all data in the index. (WARNING: 3.3M results)

Find help on Elasticsearch documentation here https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html.
