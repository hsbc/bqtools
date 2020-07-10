This is bqtools a handy set of utils to help create big query tables from json objects and make them into tables (so get the schema) and to create json new line files.
Plus a set of common functions to ease writing biq query python code

Goals
* Simplify handling and move between big query stryuctured data and json data
* Allow you to create big query table schemas from json structures (as resource or schema objects) uses reflection of types
* Provides easy generation of views for day partitioned tabled
  * head - latest data
  * diff views for time snap shots of data i.e. each day partition has current view of data
* Calculate valid json structures from representative json data that can be used as basis of big query schemas
* Clean json data such that it can be loaded into big query
  * Replace bare lists with dictionaries
  * Replace field  names with valid values that can be column names in big query (removes spaces, characters not allowed in field names using same algorithms big query uses when auto detecting schemas)
  * Encodes json output of dates, datetimes, times, timedeltas encoded in format acceptable for big query corresponding field types
* Generate code for bq command line tool from bq table structures
* Simplify common tasks of handling big query data
  * Basic tests on dataset or tables existing
  * Schema patching compare an existing schema to a template json object calculate if changed and generate a merged schema that can be used in a patch
  * Flattening views to avoid view depth limits
  
```
import bqtools

# if you load a json object say something like
foo = {
        "id":1,
        "description":""
        "aboolean":False
      }
      
# generate a schema
table = {
   "type":"TABLE",
   "location":os.environ["location"],
   "tableReference":{
       "projectId": os.environ["projectid"],
       "datasetId": os.environ["dataset"],
       "tableId": key
   },
   "timePartitioning":{
       "type": "DAY",
       "expirationMs": "94608000000"
   },
   "schema": {}
}

# use bqtools to create a schema structure
table["schema"]["fields"] = bqtools.get_bq_schema_from_json_repr(foo)

```

Demonstrates some of power of tools via [bqsync](https://github.com/MikeMoore63/bqtools/blob/master/BQSYNCUSAGE.md) that is installed if you install via pip.

    pip install bqtools-json
    
Or you can find the source for this [here](https://github.com/MikeMoore63/bqtools/blob/master/bqtools/bqsync)
    
Also provides means to handle views across environments supports SQL views based upon Jinja templates. So you can "configure" views for different environments.

Provides support for calculating view dependencies such views can be applied in parallel tranches.

Provides support for calculating authorised views based on set of views "compiled"

Compilation flattens views to lower view depth but automatically does not flatten authorised views to keep access to data tight as feasible.