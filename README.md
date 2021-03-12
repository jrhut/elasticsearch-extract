# Elasticsearch Query Extractor

Run Elasticsearch queries from the command line!

# Getting Started

In the repository directory run the command below to install the requiered packages.

```
pip install -r requirements.txt
```

Then create a .env file in the same directory, containing the variables

```
ELASTIC_HOST=?
ELASTIC_PORT=?
ELASTIC_USER=?
ELASTIC_SECRET=?
```
  
# Usage

For a short description of arguments you can use

```
python elastic_extract.py --help
```

The most basic query is a preset called --match_all that gathers all tweets and returns the fields chosen

```
python elastic_extract.py --match_all --fields "{FIELD 1} {FIELD ...}"
```

You can combine this with the --start and --end to get all tweets in some date rangethe date format used is YYYY-MM-DD. For end date you can also use "now" to use the current date.

```
python elastic_extract.py --match_all --fields "{FIELD 1} {FIELD ...} --start "{START DATE}" --end "{END DATE}"
```

The general format of a string search looks something like this

```
python elastic_extract.py --search "{FIELD}" "{SEARCH TERMS}" --fields "{FIELD 1} {FIELD 2} {FIELD ...}" --start "{START DATE}" --end "{END DATE}"
```

The general format of an exists search looks like this

```
python elastic_extract.py --exists "{FIELD}" --fields "{FIELD 1} {FIELD 2} {FIELD ...}" --start "{START DATE}" --end "{END DATE}"
```

You can combine them using --AND or a --OR argument

```
python elastic_extract.py --search "{FIELD}" "{SEARCH TERMS}" --AND --exists "{FIELD}" --fields "{FIELD 1} {FIELD 2} {FIELD ...}"

python elastic_extract.py --search "{FIELD}" "{SEARCH TERMS}" --OR --exists "{FIELD}" --fields "{FIELD 1} {FIELD 2} {FIELD ...}"
```

You can chain multiple searches or exists together too

```
python elastic_extract.py --search "{FIELD}" "{SEARCH TERMS}" --AND --search "{FIELD 2}" "{SEARCH TERMS}" --fields "{FIELD 1} {FIELD 2} {FIELD ...}"
```

# Examples

```
python elastic_extract.py --match_all --fields "user.id conversation_id entities.urls.expanded_url" --start "2020-01-01" --end "now"
```

```
python elastic_extract.py --search "full_text" "vac* OR vax*" --AND --exists "entities.urls.expanded_url" --fields "user.id full_text" --start "2020-09-13" --end "now"
```
