# bqsync
A utility for synchronising big query datasets between regions.
Note in a copy assumption is that all source projects and datasets are in 1 region and all destination projects and datasets are also in the same region. The source region and destination region MAY be different.
 
basic usage is as follows
 
    bqsync--src_project_datasets=[project1.dataset1,project1.dataset2,project2.dataset3] \
    --dst_project_datasset=[project1.dataset4,project2.dataset5,project3.dataset6] \
    --src_bucket=bucket_name --dst_bucket=another_bucket_name \
    --copy_data=True \
    --copy_views=True \
    --remove_deleted_tables=True
 
Arguments
 
*src_project_datasets* - A comma separated list of projects and datasets to copy to the destination
projects and datasets. Each item in list is project followed by a period '.' and then the dataset.
The list in this argument **MUST** contain same number as in the *dst_project_dataset* argument.
*dst_project_datasets* - A comma separated list of projects and datasets to copy into. These maybe
in a different region if they are then the *src_bucket* and *dst_bucket* **MUST** be specified.
 
*src_bucket* - If the source set of datasets and region are different to the destination datasets
region data will be extracted from source dataset into this bucket meaning it **MUST** be in the
same region as the source datasets when copying across regions.
 
*dst_bucket* - if the destination set is different to the source the data extracted into src_bucket
will rewritten to this destination bucket and then loaded into the destination datasets. It **MUST**
match the region of the target datasets.
 
*copy_data* - Default is True bqsync will copy data if set to False will only copy schemas
 
*copy_views* - Default is True bqsync will attempt to adjust views (i.e. change projects and dataset
names in the views) such they work in the new region. If views cannot be created it will skip the view
and move to the next logging an error. If set to False views are not recreated.
 
*remove_deleted_tables* - defaults to True removes tables/views in destination datset that do not exist
in source dataset
 
bqsync attempts to optimise synchronisation by comparing row numbers, bytes and last modified times for non
partitioned tables. 
For partitioned tables it compares each partition (row numbers, andaverage hash snd stdev hash 
column(s) who lowercase with regexp search (does not need to be at start) matching  (modifi.\*time,
update.\*time, creat\*time) so fields like lastModifiedtime or lastUpdatedTime or modificationTime would all match
these) in both locations, only partitions with a mismatch are copied otherwise skipped. It copies within big query 
if source and destination regions are identical.
 
bqsync relies on application default credentials and boto files for configuration (for proxy etc).
 
As such it requires sufficient permissions to run it requires the ability to run queries in both source
and destination projects, ability to create and delete and list objects in src and destination buckets and
ability to create tables and views in destination datasets.
 
bqsync is resumable i.e. it will pick up where it left off tables schemas are copied first alphabetically
and there data asynchronously (largest row sets are prioritised) then views all table copying is completed
on first pair and only when first pair is complete does it move to the next. It is assumed bqsync will be rerun to resolve issue, bqsync will update tables and schemas for schema
 changes (assumes additions).
 
Unlike dataset copying this tools has support for CMEK keys in source and destination it does this in 2major ways;
* If the source table is encrypted with a global key the key is simply reused in the destination location.
* If the source table has a regional key say HSM key then it encrypts the new table with a key of exactly
the same name with location changed to the new location. That means for thise to work the target
key **MUST** exist in the location with exactly matching name. Plus be accesible to destination projects
service account.
 
Unlike dataset copying this also puts no constraint on number of tables more tables the longer it takes.
But as resumable can be restarted at anytime.
 
NB Tested in Python 2.7 and Python 3.6 and with bigquery libraries from 0.32.0 to 1.23.1 not all features
are supported in older libraries such as clustering etc. Testing has also avoided those so far. If you
are interested in testing and improving and contributing ideas raise issues on the project or take a fork 
and propose pull requests.
 
As failures can occur it is best to make sure a lifecycle rule of delete after 1 day is set on the
buckets for usage.
 
A json file like below can be used (say this was lifeCycle.json);
 
    {
      "lifecycle": {
        "rule": [
          {
            "action": {
              "type": "Delete"
            },
            "condition": {
              "age": 1,
              "isLive": true
            }
          }
        ]
      }
    }
 
The following command would set the life cycle on the bucket
 
    gsutil lifecycle set lifeCycle.json url