#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import os
import sys

from urllib.parse import urlparse
import google.cloud.logging
from absl import app
from absl import flags
from google.cloud.logging.handlers import CloudLoggingHandler

import bqtools

FLAGS = flags.FLAGS

flags.DEFINE_bool("version", False, "Display installed version of bqsync")

flags.DEFINE_list(
    "src_project_datasets",
    [],
    """A comma separated list of projects and datasets to copy to the destination 
projects and datasets. Each item in list is project followed by a period '.' and then the dataset. 
The list in this argument MUST contain same number as in the dst_project_dataset argument""",
)

flags.DEFINE_list(
    "dst_project_datasets",
    [],
    """A comma separated list of projects and datasets to copy into. These maybe 
in a different region if they are then the src_bucket and dst_bucket MUST be specified.""",
)

flags.DEFINE_list(
    "src_ref_project_datasets",
    [],
    """A comma separated list of projects and datasets that are referenced in old 
location views,permissions. That exist and are moved outside scope of this sync but in permissions 
and views need refactoring""",
)

flags.DEFINE_list(
    "dst_ref_project_datasets",
    [],
    """A comma separated list of projects and datasets that are replacements of the 
referenced in old location views,permissions to use in new location. That exist and are moved 
outside scope of this sync but in permissions and views need refactoring""",
)

flags.DEFINE_string(
    "src_bucket",
    None,
    """If the source set of datasets and region are different to the destination 
                    datasets region data
will be extracted from source dataset into this bucket meaning it MUST be in the same region as 
the source datasets when
copying across regions.""",
)
flags.DEFINE_string(
    "log_level",
    "INFO",
    """Python logging level default is INFO set to one of DEBUG,INFO,WARNING,ERROR
to adjust logging verbosity""",
)
flags.DEFINE_string(
    "dst_bucket",
    None,
    """If the destination set is different to the source the data extracted into 
src_bucket will rewritten to this destination bucket and then loaded into the destination datasets.""",
)

flags.DEFINE_bool(
    "copy_data",
    True,
    """Default is True bqsync will copy data if set to False will only copy 
schemas""",
)

flags.DEFINE_list(
    "copy_types",
    ["TABLE", "VIEW", "ROUTINE", "MATERIALIZEDVIEW"],
    """Default is TABLE,VIEW,ROUTINE,MATERIALIZEDVIEW bqsync will attempt to sync the types specified. For example for
views/routines/model will attempt to rewrite logic. If views/routines/model cannot be created it 
will skip the view/routine/model and move to the next logging an error. If set to False views are not recreated.""",
)

flags.DEFINE_bool(
    "copy_access",
    True,
    """Copy access permissions note this has to interact with 
copy views generally if you are not copying views then only set this if no authorised views. 
Suggest if you do have these run with nocopy_data and set access and then run copying data wit
nocopy_access.""",
)

flags.DEFINE_bool(
    "remove_deleted_tables",
    True,
    """Default is True bqsync will attempt to adjust views (i.e. change projects 
and dataset names in the views) such they work in the new region. If views cannot be created it 
will skip the view and move to the next logging an error. If set to False views are not recreated.""",
)

flags.DEFINE_integer(
    "check_depth",
    -1,
    """The amount of checking that data is identcal
-2 - Check rowcounts only
-1 - Add in hash check on key times i.e. modified, updated,created, cluster fields
 0 - All level 0 root fields
 1 - All fields one below root fields etc.""",
)
flags.DEFINE_list(
    "table_or_views_to_copy",
    [".*"],
    """A comma separated list of regular expressions to match tables names against. 
For example ^a.*,^c.* would copy only views and tables starting wth a and c default is .* 
(copy everythng).""",
)

flags.DEFINE_list(
    "table_or_views_to_exclude",
    [],
    """A comma separated list of regular expressions to match tables names against. 
For example ^a.*,^c.* would exclude views and tables starting wth a and c default is empty list [] 
(exclude nothing).""",
)

flags.DEFINE_integer(
    "days_before_latest_day",
    None,
    """The number of days to check if not set  unlimted. if 7 will check between start date
minus 7 days. If 0 will just check today any negative values below 0 are not allowed""",
)

flags.DEFINE_string(
    "latest_date",
    None,
    """The day to end day partition comparison default is None i.e. end at todays date
latest date MUST be in YYYY-MM-DD format i.e. 2020-12-30""",
)

flags.DEFINE_bool(
    "do_day_partition_deep_check",
    False,
    """If set to True day partition data checks are run (this can be expensive).
default is False this will not do a check unless number of bytes differ and mod time of source > 
destination tables modification time.""",
)

flags.DEFINE_bool(
    "gcp_logging_monitoring",
    False,
    """If set to True will directly log to Global Logs->bqsync and create custom metrics for bqsync custom/bqsync/*""",
)

flags.DEFINE_string(
    "analysis_project",
    None,
    """The project that all queries will be billed too by default None and that means destination project pays for the queries
This is assumption as for example public datasets there would be no ability to run queries on the source. Setting this allows another project 
i.e. the source or in fact any other to be the charged project.""",
)

flags.DEFINE_list(
    "query_cmek",
    None,
    """This parameter defines the CMEK key touse for the queries bqsync runs. If 
not specified bqsync attempts to auto detect by using for source datasets the default kms key and 
for destination the destination datasets default kms key. The list if specified MUST be less than 
2 long.""",
)

flags.DEFINE_list(
    "src_policy_tags",
    [],
    """This paarameter is a list of source policy tags and is used with the dst_policy_tags list to defines 
a map between source and destination policy tags. if neither are set if copying within region the policy tags are copied.
If across region policy tags are not copied. This list and the dst_policy_tags MUST be the same length.""",
)

flags.DEFINE_list(
    "dst_policy_tags",
    [],
    """This parameter is a list of destination policy tags and is used with the src_policy_tags list to defines 
a map between source and destination policy tags. if neither are set if copying within region the policy tags are copied.
If across region policy tags are not copied if a mapping is provided and the mapping exists the policy tag is updated. 
This list and the dst_policy_tags MUST be the same length.""",
)


def main(argv):
    del argv  # Unused.

    # always do this if asked as soon as possible
    if FLAGS.version:
        print("Version:{}".format(bqtools.__version__))

    # validate pre-conditions
    assert len(FLAGS.src_project_datasets) > 0, (
        "Source project.dataset(s) must be set use " "--help for help"
    )

    assert len(FLAGS.dst_project_datasets) > 0, (
        "Destination project.dataset(s) must be set " "use --help for help"
    )

    assert len(FLAGS.table_or_views_to_copy) > 0, (
        "table_or_view_to_copy MUST have at least " "1 item to filter on"
    )

    assert len(FLAGS.src_policy_tags) == len(FLAGS.src_policy_tags), (
        "The list of soure "
        "(src_policy_tags) and destinationation "
        "tags(dst_policy_tags) MUSt be identical"
    )

    assert (
        FLAGS.days_before_latest_day is None or FLAGS.days_before_latest_day >= 0
    ), "Days before latest date has to be  0 or greater"

    assert FLAGS.check_depth >= -2, (
        "Check depth for data comparison MUST be > -2 see --help " "for details"
    )
    assert FLAGS.log_level in ["DEBUG", "INFO", "WARNING", "ERROR"], (
        "log_level MUST be one of DEBUG," "INFO,WARNING or ERROR"
    )

    for type in FLAGS.copy_types:
        assert type in [
            "TABLE",
            "VIEW",
            "ROUTINE",
            "MATERIALIZEDVIEW",
        ], "copy_types list items must be one of TABLE, VIEW,MATERIALIZEDVIEW or ROUTINE"

    latest_date = None

    if FLAGS.latest_date is not None:
        date_format = "%Y-%m-%d"
        try:
            latest_date = datetime.datetime.strptime(FLAGS.latest_date, date_format)
        except ValueError:
            print(
                "Incorrect data format, {} should be YYYY-MM-DD".format(
                    FLAGS.latest_date
                )
            )
            exit(-255)

    # deal with if we have proxies, intercept certificates etc
    url = urlparse(os.environ.get("https_proxy", os.environ.get("HTTPS_PROXY", None)))

    ca_certificates_file = os.environ.get("REQUESTS_CA_BUNDLE", "system")
    if ca_certificates_file != "system":
        os.environ["REQUESTS_CA_BUNDLE"] = ca_certificates_file
    ca_certificates_file = None
    proxy_user = url.username
    proxy_host = url.hostname
    proxy = ""
    if proxy_user is not None:
        proxy = "http://{}:{}@{}:{}".format(
            url.username,
            url.password,
            url.hostname,
            url.port,
        )
    else:
        if proxy_host is not None:
            proxy = "http://{}:{}".format(
                url.hostname,
                url.port,
            )
    if proxy != "":
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy

    # convert args to lists
    src_project_datasets_list = FLAGS.src_project_datasets
    dst_project_datasets_list = FLAGS.dst_project_datasets

    # tweak loggingas wanted
    loglevel = logging.INFO
    if FLAGS.log_level == "DEBUG":
        loglevel = logging.DEBUG
    elif FLAGS.log_level == "INFO":
        loglevel = logging.INFO
    elif FLAGS.log_level == "WARNING":
        loglevel = logging.WARNING
    elif FLAGS.log_level == "ERROR":
        loglevel = logging.ERROR

    logging.basicConfig(stream=sys.stdout, level=loglevel)

    # if cloud logging is set
    # TBD:This uses httplib2 which ignore env variable :-(
    # so to work behind proxy a transport is required
    # I am leaving this for the minute as its a complete pain
    # but based on demand will fix in general I am running bqsync
    # on compute host with no proxy so I don't have an urgent need to
    # resolve this issue
    if FLAGS.gcp_logging_monitoring:
        client = google.cloud.logging.Client()
        handler = CloudLoggingHandler(client, name="bqsync")
        cloud_logger = logging.getLogger()
        cloud_logger.setLevel(loglevel)
        cloud_logger.addHandler(handler)

    # set up sync
    multi_bq_copy = bqtools.MultiBQSyncCoordinator(
        src_project_datasets_list,
        dst_project_datasets_list,
        FLAGS.src_bucket,
        FLAGS.dst_bucket,
        FLAGS.remove_deleted_tables,
        FLAGS.copy_data,
        FLAGS.copy_types,
        check_depth=FLAGS.check_depth,
        table_view_filter=FLAGS.table_or_views_to_copy,
        table_or_views_to_exclude=FLAGS.table_or_views_to_exclude,
        latest_date=latest_date,
        copy_access=FLAGS.copy_access,
        days_before_latest_day=FLAGS.days_before_latest_day,
        day_partition_deep_check=FLAGS.do_day_partition_deep_check,
        analysis_project=FLAGS.analysis_project,
        cloud_logging_and_monitoring=FLAGS.gcp_logging_monitoring,
        src_ref_project_datasets=FLAGS.src_ref_project_datasets,
        dst_ref_project_datasets=FLAGS.dst_ref_project_datasets,
        query_cmek=FLAGS.query_cmek,
        src_policy_tags=FLAGS.src_policy_tags,
        dst_policy_tags=FLAGS.dst_policy_tags,
    )

    multi_bq_copy.logger = logging

    # actually do the sync
    multi_bq_copy.sync()

    exitcode = 0

    if (
        multi_bq_copy.tables_failed_sync > 0
        or multi_bq_copy.views_failed_sync > 0
        or multi_bq_copy.extract_fails > 0
        or multi_bq_copy.load_fails > 0
        or multi_bq_copy.copy_fails > 0
        or multi_bq_copy.routines_failed_sync > 0
        or multi_bq_copy.models_failed_sync > 0
    ):
        exitcode = -1

    sys.exit(exitcode)


if __name__ == "__main__":
    app.run(main)


class mod_call:
    def __call__(self):
        return app.run(main)


sys.modules[__name__] = mod_call()
