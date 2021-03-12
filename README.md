# Elasticsearch Query Extractor

Run Elasticsearch queries from the command line!

# Getting Started

In the repository directory run the command below to install the requiered packages.

```
pip install -r requirements.txt
```

Then create a .env file in the same directory, containing the variables:\
ELASTIC_HOST=?\
ELASTIC_PORT=?\
ELASTIC_USER=?\
ELASTIC_SECRET=?
  
# Usage

For a short description of arguments you can use

```
python elastic_extract.py --help
```

# Examples

```
python elastic_extract.py --search "full_text" "vac* OR vax*" --AND --exists "entities.urls.url" 
                            --fields "id full_text" --start "2020-09-13" --end "now"
```
