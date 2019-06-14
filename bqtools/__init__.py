from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from jinja2 import Environment, select_autoescape, FileSystemLoader, TemplateNotFound
from datetime import datetime, date, timedelta, time
import re
import os
import json


INVALIDBQFIELDCHARS = re.compile(r"[^a-zA-Z0-9_]")
HEADVIEW = """#standardSQL
SELECT
  *
FROM
  `{}.{}.{}`
WHERE
  _PARTITIONTIME = (
  SELECT
    MAX(_PARTITIONTIME)
  FROM
    `{}.{}.{}`)"""

_ROOT = os.path.abspath(os.path.dirname(__file__))


class BQJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        """
        Add json encoding for date, datetime, timedelta
        :param obj: Object to encode as json
        :return:
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return (datetime.min + obj).time().isoformat()
        else:
            return super(BQJsonEncoder, self).default(obj)


class Error(Exception):
    """Base Error class."""


class InconsistentJSONStructure(Error):
    """Error for inconsistent structures"""

    CUSTOM_ERROR_MESSAGE = 'The json structure passed has inconsistent types for the same key and thus cannot be made into a BQ schema or valid json new line for loading onto big query\n{1}'

    def __init__(self, resource_name, e):
        super(InconsistentJSONStructure, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e))

class NotADictionary(Error):
    CUSTOM_ERROR_MESSAGE = 'The json structure passed is not a dictionary\n{1}'
    def __init__(self, resource_name, e):
        super(NotADictionary, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e))

def get_json_struct(jsonobj,template=None):
    """

    :param jsonobj: Object to parse and adjust so could be loaded into big query
    :param template:  An input object to use as abasis as a template defaullt no template provided
    :return:  A json object that is a template object. This can be used as input to get_bq_schema_from_json_repr
    """
    if template is None:
        template = {}
    for key in jsonobj:
        newkey = INVALIDBQFIELDCHARS.sub("_", key)
        if jsonobj[key] is None:
            continue
        if newkey not in template:
            value = None
            if isinstance(jsonobj[key],bool):
                value = False
            elif isinstance(jsonobj[key], str):
                value = ""
            elif isinstance(jsonobj[key], unicode):
                value = u""
            elif isinstance(jsonobj[key], int):
                value = 0
            elif isinstance(jsonobj[key], float):
                value = 0.0
            elif isinstance(jsonobj[key], dict):
                value = get_json_struct(jsonobj[key])
            elif isinstance(jsonobj[key], list):
                if len(jsonobj[key]) == 0:
                    value = [{}]
                else:
                    if not isinstance(jsonobj[key][0],dict):
                        nv = []
                        for vali in jsonobj[key]:
                            nv.append({"value":vali})
                        jsonobj[key]=nv
                    value = [{}]
                    for li in jsonobj[key]:
                        value[0] = get_json_struct(li,value[0])
            template[newkey]=value
        else:
            if isinstance(jsonobj[key],type(template[newkey])):
                if isinstance(jsonobj[key],dict):
                    template[key] = get_json_struct(jsonobj[key],template[newkey])
                if isinstance(jsonobj[key],list):
                    if len(jsonobj[key]) != 0:
                        if not isinstance(jsonobj[key][0], dict):
                            nv = []
                            for vali in jsonobj[key]:
                                nv.append({"value": vali})
                            jsonobj[key] = nv
                    for li in jsonobj[key]:
                        template[newkey][0] = get_json_struct(li,template[newkey][0])
            else:
                raise InconsistentJSONStructure(key)
    return template

def clean_json_for_bq(anobject):
    """

    :param object to be converted to big query json compatible format:
    :return: cleaned object
    """
    newobj = {}
    if not isinstance(anobject, dict):
        raise NotADictionary(str(anobject))
    for key in anobject:
        newkey = INVALIDBQFIELDCHARS.sub("_", key)
        value = anobject[key]
        if isinstance(value, dict):
            value = clean_json_for_bq(value)
        if isinstance(value, list):
            if len(value) != 0:
                if not isinstance(value[0], dict):
                    nv = []
                    for vali in value:
                        nv.append({"value": vali})
                    value = nv
                valllist = []
                for vali in value:
                    vali = clean_json_for_bq(vali)
                    valllist.append(vali)
                value = valllist
        newobj[newkey] = value
    return newobj

def get_bq_schema_from_json_repr(jsondict):
    """
    Generate fields structure of Big query resource if the input json structure is vallid
    :param jsondict:  a template object in json format to use as basis to create a big query schema object from
    :return: a big query schema
    """
    fields = []
    for key,data in jsondict.items():
        field = {"name":key}
        if isinstance(data, bool):
            field["type"]="BOOLEAN"
            field["mode"] = "NULLABLE"
        elif isinstance(data, str):
            field["type"] = "STRING"
            field["mode"] = "NULLABLE"
        elif isinstance(data, unicode):
            field["type"] = "STRING"
            field["mode"] = "NULLABLE"
        elif isinstance(data, int):
            field["type"] = "INTEGER"
            field["mode"] = "NULLABLE"
        elif isinstance(data, float):
            field["type"] = "FLOAT"
            field["mode"] = "NULLABLE"
        elif isinstance(data, datetime):
            field["type"] = "DATETIME"
            field["mode"] = "NULLABLE"
        elif isinstance(data, date):
            field["type"] = "DATE"
            field["mode"] = "NULLABLE"
        elif isinstance(data, time):
            field["type"] = "TIME"
            field["mode"] = "NULLABLE"
        elif isinstance(data, bytes):
            field["type"] = "BYTES"
            field["mode"] = "NULLABLE"
        elif isinstance(data, dict):
            field["type"] = "RECORD"
            field["mode"] = "NULLABLE"
            field["fields"] = get_bq_schema_from_json_repr(data)
        elif isinstance(data, list):
            field["type"] = "RECORD"
            field["mode"] = "REPEATED"
            field["fields"] = get_bq_schema_from_json_repr(data[0])
        fields.append(field)
    return fields

def generate_create_schema(resourcelist, fh):
    """
    Generates using a jinja template bash command using bq to for a set of schemas
    supports views, tables or exetrnal tables.
    The resource list is a list of tables as you would get from table.get from big query
    or generated by get_bq_schema_from_json_repr

    :param resourcelist: list of resources to genereate code for
    :param fh: file handle to output too expected to be utf-8
    :return: nothing
    """
    jinjaenv = Environment(
        loader=FileSystemLoader(os.path.join(_ROOT, 'templates')),
        autoescape=select_autoescape(['html', 'xml']),
        extensions=['jinja2.ext.do', 'jinja2.ext.loopcontrols']
    )
    objtemplate = jinjaenv.get_template("bqschema.in")
    output = objtemplate.render(resourcelist=resourcelist)
    print(output.encode('utf-8'), file=fh)

def generate_create_schema_file(filename, resourcelist):
    """
    Generates using a jinja template bash command using bq to for a set of schemas
    supports views, tables or exetrnal tables.
    The resource list is a list of tables as you would get from table.get from big query
    or generated by get_bq_schema_from_json_repr

    :param filename: filename to putput too
    :param resourcelist: list of resources to genereate code for
    :return:nothing
    """
    with open(filename, mode='wb+') as fh:
        generate_create_schema(resourcelist, fh)

def dataset_exists(self, client, dataset_reference):
    """Return if a dataset exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
            A reference to the dataset to look for.

    Returns:
        bool: ``True`` if the dataset exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False

def table_exists(self, client, table_reference):
    """Return if a table exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        table_reference (google.cloud.bigquery.table.TableReference):
            A reference to the table to look for.

    Returns:
        bool: ``True`` if the table exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_table(table_reference)
        return True
    except NotFound:
        return False
