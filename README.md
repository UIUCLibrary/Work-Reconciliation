# Setup
## Cache
This script requires Redis to run. If you do not already have it running, please follow [these instructions](https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/) to install and setup.

To connect to the cache, you need to create an `application.conf` file with the following format:
```
[redis]
host = <host>
port = <port #>
loc_db = <database #>
wiki_db = <different database #>
```

## BIBFRAME XML
This script expects a BIBFRAME XML file as input, generated from LOC's [marc2bibframe2](https://github.com/lcnetdev/marc2bibframe2) tool. Specifically, this converted MARCXML into BIBFRAME XML, but the conversion won't work unless you have `xmlns="http://www.loc.gov/MARC21/slim"` in the `collection` tag of the MARCXML file.

# Running
The command to run the script should look like
```
python reconcileWorks.py <input.xml> <output directory> <loc|wikidata>
```

# LOC Reconciliation Process
todo