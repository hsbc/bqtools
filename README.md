This is bqtools a handy set of utils to help create big query tables from json objects and make them into tables (so get the schema) and to create json new line files.
Plus a set of common functions to ease writing biq query python code

Goals
* Simplify hadling and move between big query stryuctured data and json data
* Allow you to create big query table schemas from json structures
* Provides easy generation of views for day partitioned tabled
  * head - latest data
* Calculate valid json structures from representative json data that can be used as basis of big query schemas
* Clean json data such that it can be loaded into big query
  * Replace bare lists with dictionaries
  * Replace field  names with vallid values that can be column names in big query (removes spaces, characters not allowed in field names using same algorithms big query uses when auto detecting schemas)
  * Encodes json output of dates, datetimes, times, timedeltas encoded in format acceptable for big query corresponding field types
* Generate code for bq command line tool from bq table structures
* Simplify common tasks of handling big query data
  * Basic tests on dataset or tables existing
  * Schema patching compare an existing schema to a template json object calculate if changed and generate a merged schema that can be used in a patch
    