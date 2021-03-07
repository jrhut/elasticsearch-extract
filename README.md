# Elasticsearch Query Extractor

Extracts all query results from Elasticsearch and saves it to query_output.csv!

# Getting Started

You will want to create a .env file in the repo root, containing the variables:\
ELASTIC_HOST\
ELASTIC_PORT\
ELASTIC_USER\
ELASTIC_SECRET
  
# Usage

To make a query use the command line arguments provided, see --help.

# Examples

```
python elastic_extract.py --search "full_text" "vac* OR vax*" --AND --exists "entities.urls.url" 
                            --fields "id full_text" --start "2020-09-13" --end "now"
```
