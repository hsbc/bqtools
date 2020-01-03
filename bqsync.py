# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os

import boto
import bqtools
from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS

flags.DEFINE_string('src_project_datasets', None,
                    """A comma separated list of projects and datasets to copy to the destination 
                    projects and datasets.
Each item in list is project followed by a period '.' and then the dataset. The list in this
argument MUST contain same number as in the dst_project_dataset argument""")
flags.DEFINE_string('dst_project_datasets', None,
                    """A comma separated list of projects and datasets to copy into. These maybe 
                    in a different region
if they are then the src_bucket and dst_bucket MUST be specified.""")
flags.DEFINE_string('src_bucket', None,
                    """If the source set of datasets and region are different to the destination 
                    datasets region data
will be extracted from source dataset into this bucket meaning it MUST be in the same region as 
the source datasets when
copying across regions.""")
flags.DEFINE_string('dst_bucket', None,
                    """If the destination set is different to the source the data extracted into 
                    src_bucket will
rewritten to this destination bucket and then loaded into the destination datasets.""")
flags.DEFINE_bool('copy_data', True,
                  """Default is True bqsync will copy data if set to False will only copy 
                  schemas""")
flags.DEFINE_bool('copy_views', True,
                  """Default is True bqsync will attempt to adjust views (i.e. change projects 
                  and dataset names in
the views) such they work in the new region. If views cannot be created it will skip the view and 
move to the next
logging an error. If set to False views are not recreated.""")

flags.DEFINE_bool('remove_deleted_tables', True,
                  """Default is True bqsync will attempt to adjust views (i.e. change projects 
                  and dataset names in
the views) such they work in the new region. If views cannot be created it will skip the view and 
move to the next
logging an error. If set to False views are not recreated.""")


def main(argv):
    del argv  # Unused.

    # validate pre-conditions
    assert FLAGS.src_project_datasets is not None, "Source project.dataset(s) must be set use " \
                                                   "--help for help"
    assert FLAGS.dst_project_datasets is not None, "Destination project.dataset(s) must be set " \
                                                   "use --help for help"

    # deal with if we have proxies, intercept certificates etc
    ca_certificates_file = boto.config.get('Boto', 'ca_certificates_file', "system")

    if ca_certificates_file != "system":
        os.environ["REQUESTS_CA_BUNDLE"] = ca_certificates_file

    proxy_user = boto.config.get('Boto', 'proxy_user', None)
    proxy_host = boto.config.get('Boto', 'proxy', None)
    proxy = ""
    if proxy_user is not None:
        proxy = "http://{}:{}@{}:{}".format(boto.config.get('Boto', 'proxy_user', None),
                                            boto.config.get('Boto', 'proxy_pass', None),
                                            boto.config.get('Boto', 'proxy', None),
                                            boto.config.getint('Boto', 'proxy_port', 0))
    else:
        if proxy_host is not None:
            proxy = "http://{}:{}".format(boto.config.get('Boto', 'proxy', None),
                                          boto.config.getint('Boto', 'proxy_port', 0))
    if proxy != "":
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy

    # convert args to lists
    src_project_datasets_list = FLAGS.src_project_datasets.split(",")
    dst_project_datasets_list = FLAGS.dst_project_datasets.split(",")

    # set up sync
    multi_bq_copy = bqtools.MultiBQSyncCoordinator(src_project_datasets_list,
                                                   dst_project_datasets_list,
                                                   FLAGS.src_bucket,
                                                   FLAGS.dst_bucket,
                                                   FLAGS.remove_deleted_tables,
                                                   FLAGS.copy_data,
                                                   FLAGS.copy_views)

    # tweak logging
    multi_bq_copy.logger = logging

    # actually do the sync
    multi_bq_copy.sync()

    # provide some stats
    if multi_bq_copy.rows_synced - multi_bq_copy.rows_avoided == 0:
        speed_up = "Infinity"
    else:
        speed_up = float(multi_bq_copy.rows_synced) / float(
            multi_bq_copy.rows_synced - multi_bq_copy.rows_avoided)

        multi_bq_copy.logger.info(
            "Tables synced {}, Views synced {}, Rows synced {}, Rows Avoided {}, speed up {"
            "}".format(
                multi_bq_copy.tables_synced, multi_bq_copy.views_synced, multi_bq_copy.rows_synced,
                multi_bq_copy.rows_avoided, speed_up))


if __name__ == '__main__':
    app.run(main)
