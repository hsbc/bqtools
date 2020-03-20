# -*- coding: utf-8 -*-
"""bqtools-json a module for managing interaction between json data and big query.

This module provides utility functions for big query and specifically treating big query as json
document database.
Schemas can be defined in json and provides means to create such structures by reading or passing
json structures.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import concurrent
import copy
import json
import logging
import os
import pprint
import random
import re
import threading
import warnings
from datetime import datetime, date, timedelta, time as dttime
from io import open
from time import sleep

import requests
# handle python 2 and 3 versions of this
import six
from google.cloud import bigquery, exceptions, storage
from jinja2 import Environment, select_autoescape, FileSystemLoader
from six.moves import queue

# import logging

INVALIDBQFIELDCHARS = re.compile(r"[^a-zA-Z0-9_]")
HEADVIEW = """#standardSQL
SELECT
  *
FROM
  `{0}.{1}.{2}`
WHERE
  _PARTITIONTIME = (
  SELECT
    MAX(_PARTITIONTIME)
  FROM
    `{0}.{1}.{2}`)"""

_ROOT = os.path.abspath(os.path.dirname(__file__))

# get a list of tables for a given dataset in table name
# sorted by name so can be compared with anpther list
DSTABLELISTQUERY = """SELECT
  table_name
FROM
  `{}.{}.INFORMATION_SCHEMA.TABLES`
WHERE
  table_type = "BASE TABLE"
ORDER BY
  1"""

# get views in creation time order
# idea behind this is views can only be created in order
# so in theory the date order is the right order.
# however view updates could mess this up
# but lets try this if it generally works grea
DSVIEWLISTQUERY = """SELECT
  v.table_name,
  v.use_standard_sql,
  t.creation_time,
  v.view_definition
FROM
  `{0}.{1}.INFORMATION_SCHEMA.TABLES` AS t
JOIN
  `{0}.{1}.INFORMATION_SCHEMA.VIEWS` AS v
ON
  v.table_name = t.table_name
WHERE
  t.table_type = "VIEW"
ORDER BY
  v.table_name"""

DSVIEWORDER = """SELECT
  t.table_name,"VIEW" as type,creation_time
FROM
  `{0}.{1}.INFORMATION_SCHEMA.TABLES` AS t
WHERE
  t.table_type = "VIEW"
UNION ALL 
SELECT
  routine_name as table_name,"ROUTINE" as type,last_altered as creation_time
FROM
   `{0}.{1}.INFORMATION_SCHEMA.ROUTINES`
ORDER BY
  creation_time
"""

RTNCOMPARE = """SELECT
  routine_name,
  routine_body,
  routine_type,
  data_type,
  routine_definition
FROM
  `{0}.{1}.INFORMATION_SCHEMA.ROUTINES`
ORDER BY
  1"""

TCMPDAYPARTITION = """SELECT
  FORMAT_TIMESTAMP("%Y%m%d", {partitiontime}) AS partitionName,
  COUNT(*) AS rowNum{extrafunctions}
FROM
  `{project}.{dataset}.{table_name}` as zzz{extrajoinandpredicates}
GROUP BY
  1
ORDER BY
  1"""

# mapping of big query regions to kms key regions if mapping required
# if no mapping just lower cases the dataset region and it will match is assumption
# when copying between regions the kms name is assumed the same bar the region
# keys are not compared in idffs and patching is feasible after the fact
MAPBQREGION2KMSREGION = {
    "EU": "europe"
}

BQSYNCQUERYLABELS = {
    "bqsyncversion": "bqyncv0_4"
}


class BQJsonEncoder(json.JSONEncoder):
    """ Class to implement encoding for date times, dates and timedelta

    """

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


class BQTError(Exception):
    """Base Error class."""


class InconsistentJSONStructure(BQTError):
    """Error for inconsistent structures"""

    CUSTOM_ERROR_MESSAGE = 'The json structure passed has inconsistent types for the same key ' \
                           '{0} and thus cannot be ' \
                           'made into a BQ schema or valid json new line for loading onto big ' \
                           'query\n{1} type ' \
                           'previously {2}'

    def __init__(self, resource_name, e, ttype):
        super(InconsistentJSONStructure, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e, ttype))


class NotADictionary(BQTError):
    CUSTOM_ERROR_MESSAGE = 'The json structure passed is not a dictionary\n{1}'

    def __init__(self, resource_name):
        super(NotADictionary, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e))


class UnexpectedType(BQTError):
    CUSTOM_ERROR_MESSAGE = 'The object type of \n{1} is nota type that bqutils knows how to handle'

    def __init__(self, resource_name):
        super(UnexpectedType, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e))


class UnexpectedDict(BQTError):
    CUSTOM_ERROR_MESSAGE = 'The object is a dict and shoulld not be \n{0}'

    def __init__(self, resource_name):
        super(UnexpectedDict, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name))


class SchemaMutationError(BQTError):
    """Error for API executions."""

    CUSTOM_ERROR_MESSAGE = 'Schema Mutation Error: unable to mutate object path {0} on keyi {1} ' \
                           'object {2}'

    def __init__(self, objtomatch, keyi, path):
        super(SchemaMutationError, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(objtomatch, keyi, path))


class BQQueryError(BQTError):
    """Error for Query execution error."""
    CUSTOM_ERROR_MESSAGE = 'GCP API Error: unable to processquery {0} from GCP:\n{1}'

    def __init__(self, query, desc, e):
        super(ApiExecutionError, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(query, e))


class BQSyncTask(object):
    def __init__(self, function, args):
        assert callable(function), "Tasks must be constructed with a function"
        assert isinstance(args, list), "Must have arguments"
        self.__function = function
        self.__args = args

    @property
    def function(self):
        return self.__function

    @property
    def args(self):
        return self.__args

    def __eq__(self, other):
        return self.args == other.args

    def __lt__(self, other):
        return self.args < other.args

    def __gt__(self, other):
        return self.args > other.args


def get_json_struct(jsonobj, template=None):
    """

    :param jsonobj: Object to parse and adjust so could be loaded into big query
    :param template:  An input object to use as abasis as a template defaullt no template provided
    :return:  A json object that is a template object. This can be used as input to
    get_bq_schema_from_json_repr
    """
    if template is None:
        template = {}
    for key in jsonobj:
        newkey = INVALIDBQFIELDCHARS.sub("_", key)
        if jsonobj[key] is None:
            continue
        if newkey not in template:
            value = None
            if isinstance(jsonobj[key], bool):
                value = False
            elif isinstance(jsonobj[key], six.string_types):
                value = ""
            elif isinstance(jsonobj[key], six.text_type):
                value = u""
            elif isinstance(jsonobj[key], int) or isinstance(jsonobj[key], long):
                value = 0
            elif isinstance(jsonobj[key], float):
                value = 0.0
            elif isinstance(jsonobj[key], date):
                value = jsonobj[key]
            elif isinstance(jsonobj[key], datetime):
                value = jsonobj[key]
            elif isinstance(jsonobj[key], dict):
                value = get_json_struct(jsonobj[key])
            elif isinstance(jsonobj[key], list):
                value = [{}]
                if len(jsonobj[key]) > 0:
                    if not isinstance(jsonobj[key][0], dict):
                        new_value = []
                        for vali in jsonobj[key]:
                            new_value.append({"value": vali})
                        jsonobj[key] = new_value
                    for list_item in jsonobj[key]:
                        value[0] = get_json_struct(list_item, value[0])
            else:
                raise UnexpectedType(str(jsonobj[key]))
            template[newkey] = value
        else:
            if isinstance(jsonobj[key], type(template[newkey])):
                if isinstance(jsonobj[key], dict):
                    template[key] = get_json_struct(jsonobj[key], template[newkey])
                if isinstance(jsonobj[key], list):
                    if len(jsonobj[key]) != 0:
                        if not isinstance(jsonobj[key][0], dict):
                            new_value = []
                            for vali in jsonobj[key]:
                                new_value.append({"value": vali})
                            jsonobj[key] = new_value
                        for list_item in jsonobj[key]:
                            template[newkey][0] = get_json_struct(list_item, template[newkey][0])
            else:
                # work out best way to loosen types with worst case change to string
                newtype = ""
                if isinstance(jsonobj[key], float) and isinstance(template[newkey], int):
                    newtype = 0.0
                elif isinstance(jsonobj[key], datetime) and isinstance(template[newkey], date):
                    newtype = jsonobj[key]
                if not (isinstance(jsonobj[key], dict) or isinstance(jsonobj[key], list)) and not (
                        isinstance(template[newkey], list) or isinstance(template[newkey], dict)):
                    template[newkey] = newtype
                else:
                    # this is so different type cannot be loosened
                    raise InconsistentJSONStructure(key, str(jsonobj[key]), str(template[newkey]))
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
                    new_value = []
                    for vali in value:
                        new_value.append({"value": vali})
                    value = new_value
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
    :param jsondict:  a template object in json format to use as basis to create a big query
    schema object from
    :return: a big query schema
    """
    fields = []
    for key, data in jsondict.items():
        field = {"name": key}
        if isinstance(data, bool):
            field["type"] = "BOOLEAN"
            field["mode"] = "NULLABLE"
        elif isinstance(data, six.string_types):
            field["type"] = "STRING"
            field["mode"] = "NULLABLE"
        elif isinstance(data, six.text_type):
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
        elif isinstance(data, dttime):
            field["type"] = "TIME"
            field["mode"] = "NULLABLE"
        elif isinstance(data, six.binary_type):
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


def generate_create_schema(resourcelist, file_handle):
    """
    Generates using a jinja template bash command using bq to for a set of schemas
    supports views, tables or exetrnal tables.
    The resource list is a list of tables as you would get from table.get from big query
    or generated by get_bq_schema_from_json_repr

    :param resourcelist: list of resources to genereate code for
    :param file_handle: file handle to output too expected to be utf-8
    :return: nothing
    """
    jinjaenv = Environment(
        loader=FileSystemLoader(os.path.join(_ROOT, 'templates')),
        autoescape=select_autoescape(['html', 'xml']),
        extensions=['jinja2.ext.do', 'jinja2.ext.loopcontrols']
    )
    objtemplate = jinjaenv.get_template("bqschema.in")
    output = objtemplate.render(resourcelist=resourcelist)
    print(output, file=file_handle)


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
    with open(filename, mode='w+',encoding="utf-8") as file_handle:
        generate_create_schema(resourcelist, file_handle)


def dataset_exists(client, dataset_reference):
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
    except exceptions.NotFound:
        return False


def table_exists(client, table_reference):
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
    except exceptions.NotFound:
        return False


def create_schema(sobject, schema_depth=0, fname=None, dschema=None):
    schema = []
    if dschema is None:
        dschema = {}
    dummyfield = bigquery.SchemaField('xxxDummySchemaAsNoneDefinedxxx', 'STRING')

    if fname is not None:
        fname = INVALIDBQFIELDCHARS.sub("_", fname)
    if isinstance(sobject, list):
        tschema = []
        # if fname is not None:
        #    recordschema = bigquery.SchemaField(fname, 'RECORD', mode='REPEATED')
        #    # recordschema.fields = tschema
        #    ok so scenarios to handle here are
        #    creating a schema from a delliberate schema object for these we know
        #    there will be only 1 item in the ist with al fields
        #    but also we have creating a schema from an object which is just
        #    an object so could have in a list more than 1 item and items coudl have
        #    different fiedls
        #
        pdschema = dschema
        if fname is not None and fname not in dschema:
            dschema[fname] = {}
            pdschema = dschema[fname]
        for i in sobject:
            # lists must have dictionaries and not base types
            # if not a dictionary skip
            if isinstance(sobject, dict) or isinstance(sobject, list):
                tschema.extend(create_schema(i, dschema=pdschema))
        if len(tschema) == 0:
            tschema.append(dummyfield)
        if fname is not None:
            recordschema = bigquery.SchemaField(fname, 'RECORD', mode='REPEATED', fields=tschema)
            # recordschema.fields = tuple(tschema)
            schema.append(recordschema)
        else:
            schema = tschema


    elif isinstance(sobject, dict):
        tschema = []
        # if fname is not None:
        #    recordschema = bigquery.SchemaField(fname, 'RECORD')
        #    recordschema.fields = tschema
        if len(sobject) > 0:
            for j in sobject:
                if j not in dschema:
                    dschema[j] = {}
                if "simple" not in dschema[j]:
                    fieldschema = create_schema(sobject[j], fname=j, dschema=dschema[j])
                    if fieldschema is not None:
                        if fname is not None:
                            tschema.extend(fieldschema)
                        else:
                            schema.extend(fieldschema)
        else:
            if fname is not None:
                tschema.append(dummyfield)
            else:
                schema.append(dummyfield)

        if fname is not None:
            recordschema = bigquery.SchemaField(fname, 'RECORD', fields=tschema)
            schema = [recordschema]


    else:
        fieldschema = None
        if fname is not None:
            if isinstance(sobject, bool):
                fieldschema = bigquery.SchemaField(fname, 'BOOLEAN')
            elif isinstance(sobject, int):
                fieldschema = bigquery.SchemaField(fname, 'INTEGER')
            elif isinstance(sobject, float):
                fieldschema = bigquery.SchemaField(fname, 'FLOAT')
            # start adtes and times at lowest levelof hierarchy
            # https://docs.python.org/3/library/datetime.html subclass
            # relationships
            elif isinstance(sobject, datetime):
                fieldschema = bigquery.SchemaField(fname, 'DATETIME')
            elif isinstance(sobject, date):
                fieldschema = bigquery.SchemaField(fname, 'DATE')
            elif isinstance(sobject, dttime):
                fieldschema = bigquery.SchemaField(fname, 'TIME')
            elif isinstance(sobject, six.string_types) or isinstance(sobject, six.text_type):
                fieldschema = bigquery.SchemaField(fname, 'STRING')
            elif isinstance(sobject, six.binary_type):
                fieldschema = bigquery.SchemaField(fname, 'BYTES')
            elif isinstance(sobject, list):
                # Big query cannot support non templated lists
                # Will cause error 'Failed to create table: Field bill is
                # type RECORD but has no schema'
                if len(sobject) > 0:
                    fieldschema = bigquery.SchemaField(fname, 'RECORD')
                    fieldschema.mode = 'REPEATED'
                    mylist = sobject
                    head = mylist[0]
                    fieldschema.fields = create_schema(head, schema_depth + 1)
            elif isinstance(sobject, dict):
                fieldschema = bigquery.SchemaField(fname, 'RECORD')
                fieldschema.fields = create_schema(sobject, schema_depth + 1)
            else:
                raise UnexpectedType(str(type(sobject)))
            if dschema is not None:
                dschema["simple"] = True
            return [fieldschema]
        else:
            return []

    return schema


# convert a dict and with a schema object to assict convert dict into tuple
def dict_plus_schema_2_tuple(data, schema):
    """
    :param data:
    :param schema:
    :return:
    """
    otuple = []

    # must  iterate through schema to add Nones so dominates
    for schema_item in schema:
        value = None
        if data is not None and schema_item.name in data:
            value = data[schema_item.name]
        if schema_item.field_type == 'RECORD':
            ttuple = []
            if schema_item.mode != 'REPEATED' or value is None:
                value = [value]
            for value_item in value:
                value = dict_plus_schema_2_tuple(value_item, schema_item.fields)
                ttuple.append(value)
            value = ttuple
        otuple.append(value)

    return tuple(otuple)


# so assumes a list containing list of lists for structs and
# arras but for an arra of structs value is always an array
def tuple_plus_schema_2_dict(data, schema):
    """
    :param data:
    :param schema:
    :return:
    """
    rdata = {}
    for schema_item, value in zip(schema, data):
        if schema_item.field_type == 'RECORD':
            ldata = []
            if schema_item.mode == 'REPEATED':
                llist = value
            else:
                llist = [value]
            for list_item in llist:
                ldata.append(tuple_plus_schema_2_dict(list_item, schema_item.fields))
            if schema_item.mode == 'REPEATED':
                value = ldata
            else:
                value = ldata[0]
        rdata[schema_item.name] = value

    return rdata


def gen_template_dict(schema):
    """

    :param schema: Take a rest representation of google big query table fields and create a
    template json object
    :return:
    """
    rdata = {}
    for schema_item in schema:
        value = None
        if schema_item.field_type == 'RECORD':
            tvalue = self.gen_template_dict(schema_item.fields)
            if schema_item.mode == 'REPEATED':
                value = [tvalue]
            else:
                value = tvalue
        elif schema_item.field_type == 'INTEGER':
            value = 0
        elif schema_item.field_type == 'BOOLEAN':
            value = False
        elif schema_item.field_type == 'FLOAT':
            value = 0.0
        elif schema_item.field_type == 'STRING':
            value = ""
        elif schema_item.field_type == 'DATETIME':
            value = datetime.utcnow()
        elif schema_item.field_type == 'DATE':
            value = date.today()
        elif schema_item.field_type == 'TIME':
            value = datetime.time()
        elif schema_item.field_type == 'BYTES':
            value = b'\x00'
        else:
            raise UnexpectedType(str(type(sobject)))
        rdata[schema_item.name] = value

    return rdata


def to_dict(schema):
    field_member = {"name": schema.name,
                    "type": schema.field_type,
                    "description": schema.description,
                    "mode": schema.mode,
                    "fields": None}
    if schema.fields is not None:
        fields_to_append = []
        for field_item in schema.fields:
            fields_to_append.append(to_dict(field_item))
        field_member['fields'] = fields_to_append
    return field_member


def calc_field_depth(fieldlist, depth=0):
    max_depth = depth
    recursive_depth = depth
    for i in fieldlist:
        if 'fields' in i:
            recursive_depth = calc_field_depth(i['fields'], depth + 1)
            if recursive_depth > max_depth:
                max_depth = recursive_depth
    return max_depth


def trunc_field_depth(fieldlist, maxdepth, depth=0):
    new_field = []
    if depth <= maxdepth:
        for i in fieldlist:
            new_field.append(i)
            if 'fields' in i:
                if depth == maxdepth:
                    # json.JSONEncoder().encode(fieldlist)
                    i['type'] = 'STRING'
                    i.pop('fields', None)
                else:
                    i['fields'] = trunc_field_depth(
                        i['fields'], maxdepth, depth + 1)

    return new_field


def match_and_addtoschema(objtomatch, schema, evolved=False, path="", logger=None):
    pretty_printer = pprint.PrettyPrinter(indent=4)
    poplist = {}

    for keyi in objtomatch:
        # Create schema does this adjustment so we need to do same in actual object
        thekey = INVALIDBQFIELDCHARS.sub('_', keyi)
        # Work out if object keys have invalid values and n
        if thekey != keyi:
            poplist[keyi] = thekey
        matchstruct = False
        # look for bare list should not have any if known about
        # big query cannot hande bare lists
        # so to alow schema evoution MUST be removed
        # this test if we have a list and a value in it is it a bare type i.e.not a dictionary
        # if it is not a dictionary use bare type ist method to cnvert to a dictionary
        # where object vallue is a singe key in a dict of value
        # this changes each object as well meaning they will load into the evolved schema
        # we call this with log error false as this method checks if the key exists and
        # if the object is a list and lengh > 0 and if the object at the end is dict or not only
        # converts if not a dict
        # this is important optimisation as if we checked here it would be a double check
        # as lots of objects this overhead is imprtant to minimise hence why this
        # looks like it does
        do_bare_type_list(objtomatch, keyi, "value")
        for schema_item in schema:
            if thekey == schema_item.name:
                if schema_item.field_type == 'RECORD':
                    if schema_item.mode == 'REPEATED':
                        subevolve = evolved
                        for listi in objtomatch[keyi]:
                            # TODO hack to modify fields as .fields is immutable since version
                            #  0.28 and later but not
                            #  in docs!!
                            schema_item._fields = list(schema_item.fields)
                            tsubevolve = match_and_addtoschema(listi, schema_item.fields,
                                                               evolved=evolved,
                                                               path=path + "." + thekey)
                            if not subevolve and tsubevolve:
                                subevolve = tsubevolve
                        evolved = subevolve
                    else:
                        # TODO hack to modify fields as .fields is immutable since version 0.28
                        #  and later but not in
                        #  docs!!
                        schema_item._fields = list(schema_item.fields)
                        evolved = match_and_addtoschema(objtomatch[keyi], schema_item.fields,
                                                        evolved=evolved)
                matchstruct = True
                break
        if matchstruct:
            continue

        # Construct addition to schema here based on objtomatch[keyi] schema or object type
        # append to the schema list
        try:
            toadd = create_schema(objtomatch[keyi], fname=keyi)
        except Exception as an_exception:
            raise SchemaMutationError(str(objtomatch), keyi, path)

        if toadd is not None:
            schema.extend(toadd)
            if logger is not None:
                logger.warning(
                    u"Evolved path = {}, struct={}".format(path + "." + thekey,
                                                           pretty_printer.pformat(
                                                               objtomatch[keyi])))
            evolved = True

    # If values of keys did need changing change them
    if len(poplist):
        for pop_item in poplist:
            objtomatch[poplist[pop_item]] = objtomatch[pop_item]
            objtomatch.pop(pop_item, None)

    return evolved


def do_bare_type_list(adict, key, detail, logger=None):
    """
    Converts a list that is pointed to be a key in a dctionary from
    non dictionary object to dictionary object. We do this as bare types
    are not allowed in BQ jsons structures. So structures of type

    "foo":[ 1,2,3 ]

    to

    "foo":[{"detail":1},{"detail":2},{"detail":3}]

    Args:
        self: The gscanner object instance
        adict: The dictionary the key of the list object is in. This object is modified so mutated.
        key: The key name of the list if it does not exist this does nothing. if the item at the
        key is not a list it
        does nothing if length of list is 0 this does nothing
        detail: The name of the field in new sub dictionary of each object
        logerror: boolean if true and list objects are already a dictionary will log trace and
        key that ha dthe issue


    Returns:
        Nothing.

    Raises:
        Nothing
    """
    try:
        if key in adict:
            if key in adict and isinstance(adict[key], list) and len(adict[key]) > 0:
                if not isinstance(adict[key][0], dict):
                    new_list = []
                    for list_item in adict[key]:
                        new_list.append({detail: list_item})
                    adict[key] = new_list
                else:
                    if logger is not None:
                        tbs = traceback.extract_stack()
                        tbsflat = "\n".join(map(str, tbs))
                        logger.error(
                            "Bare list for key {} in dict {} expected a basic type not converting "
                            "{}".format(
                                key,
                                str(adict),
                                tbsflat))
    except Exception as an_exception:
        raise UnexpectedDict(
            "Bare list for key {} in dict {} expected a basic type not converting".format(key, str(
                adict)))


def recurse_and_add_to_schema(schema, oschema):
    changes = False

    # Minimum is new schema now this can have less than old
    wschema = copy.deepcopy(schema)

    # Everything in old schema stays as a patch
    for output_schema_item in oschema:
        nschema = []
        # Look for
        for new_schema_item in wschema:
            if output_schema_item['name'].lower() == new_schema_item.name.lower():
                if output_schema_item['type'] == 'RECORD':
                    rchanges, output_schema_item['fields'] = \
                        recurse_and_add_to_schema(new_schema_item.fields,
                                                  output_schema_item[
                                                      'fields'])
                    if rchanges and not changes:
                        changes = rchanges
            else:
                nschema.append(new_schema_item)
        wschema = nschema

    # Now just has what remain in it.
    for wsi in wschema:
        changes = True
        oschema.append(to_dict(wsi))

    return (changes, oschema)


FSLST = """#standardSQL
SELECT 
     ut.*, 
     fls.firstSeenScanversion,
     fls.lastSeenScanVersion,
     fls.firstSeenTime,
     fls.lastSeenTime,
     fls.numSeen
FROM `{0}.{1}.{2}` as ut
JOIN (
    SELECT 
       id,
       {4} AS firstSeenTime,
       {4} AS lastSeenTime,
       COUNT(*) AS numSeen
    FROM `{0}.{1}.{2}`
    GROUP BY 
    1) AS fls
ON fls.id = ut.id AND fls.{3}  = {4}
"""
FSLSTDT = "View that shows {} captured values of underlying table for object of a " \
          "given non repeating key " \
          "of 'id' {}.{}.{}"


def gen_diff_views(project,
                   dataset,
                   table,
                   schema,
                   description="",
                   intervals=None,
                   update_only_fields=None,
                   time_expr=None,
                   fieldsappend=None):
    """

    :param project: google project id of underlying table
    :param dataset: google dataset id of underlying table
    :param table: the base table to do diffs (assumes each time slaice is a view of what data
    looked like))
    :param schema: the schema of the base table
    :param description: a base description for the views
    :param intervals: a list of form []
    :param update_only_fields:
    :param time_expr:
    :param fieldsappend:
    :return:
    """

    views = []
    fieldsnot4diff = []
    if intervals is None:
        intervals = [{"day": "1 DAY"}, {"week": "7 DAY"}, {"month": "30 DAY"},
                     {"fortnight": "14 DAY"}]

    if time_expr is None:
        time_expr = "_PARTITIONTIME"
    fieldsnot4diff.append("scantime")
    if isinstance(fieldsappend, list):
        for fdiffi in fieldsappend:
            fieldsnot4diff.append(fdiffi)
    if update_only_fields is None:
        update_only_fields = ['creationTime',
                              'usage',
                              'title',
                              'description',
                              'preferred',
                              'documentationLink',
                              'discoveryLink',
                              'numLongTermBytes',
                              'detailedStatus',
                              'lifecycleState',
                              'size',
                              'md5Hash',
                              'crc32c',
                              'timeStorageClassUpdated',
                              'deleted',
                              'networkIP',
                              'natIP',
                              'changePasswordAtNextLogin',
                              'status',
                              'state',
                              'substate',
                              'stateStartTime',
                              'metricValue',
                              'requestedState',
                              'statusMessage',
                              'numWorkers',
                              'currentStateTime',
                              'currentState',
                              'lastLoginTime',
                              'lastViewedByMeDate',
                              'modifiedByMeDate',
                              'etag',
                              'servingStatus',
                              'lastUpdated',
                              'updateTime',
                              'lastModified',
                              'lastModifiedTime',
                              'timeStorageClassUpdated',
                              'updated',
                              'numRows',
                              'numBytes',
                              'numUsers',
                              'isoCountryCodes',
                              'countries',
                              'uriDescription']

    fqtablename = "{}.{}.{}".format(project, dataset, table)
    basediffview = table + "db"
    basefromclause = "\nfrom `{}` as {}".format(fqtablename, "ta" + table)
    baseselectclause = """#standardSQL
SELECT
    {} AS scantime""".format(time_expr)

    curtablealias = "ta" + table
    fieldprefix = ""
    aliasstack = []
    fieldprefixstack = []
    fields4diff = []

    # fields to ignore as in each snapshot and different even if content is the same
    fields_update_only = []
    aliasnum = 1

    basedata = {"select": baseselectclause, "from": basefromclause, "aliasnum": aliasnum}

    def recurse_diff_base(schema, fieldprefix, curtablealias):
        pretty_printer = pprint.PrettyPrinter(indent=4)

        for schema_item in schema:
            skip = False
            for fndi in fieldsnot4diff:
                if schema_item.name == fndi:
                    skip = True
                    break
            if skip:
                continue
            if schema_item.field_type == 'STRING':
                basefield = ',\n    ifnull({}.{},"None") as {}'.format(
                    curtablealias,
                    schema_item.name,
                    fieldprefix + schema_item.name)
            elif schema_item.field_type == 'BOOLEAN':
                basefield = ',\n    ifnull({}.{},False) as {}'.format(curtablealias,
                                                                      schema_item.name,
                                                                      fieldprefix +
                                                                      schema_item.name)
            elif schema_item.field_type == 'INTEGER':
                basefield = ',\n    ifnull({}.{},0) as {}'.format(curtablealias, schema_item.name,
                                                                  fieldprefix + schema_item.name)
            elif schema_item.field_type == 'FLOAT':
                basefield = ',\n    ifnull({}.{},0.0) as {}'.format(curtablealias, schema_item.name,
                                                                    fieldprefix + schema_item.name)
            elif schema_item.field_type == 'DATE':
                basefield = ',\n    ifnull({}.{},DATE(1970,1,1)) as {}'.format(curtablealias,
                                                                               schema_item.name,
                                                                               fieldprefix +
                                                                               schema_item.name)
            elif schema_item.field_type == 'DATETIME':
                basefield = ',\n    ifnull({}.{},DATETIME(1970,1,1,0,0,0)) as {}'.format(
                    curtablealias, schema_item.name,
                    fieldprefix + schema_item.name)
            elif schema_item.field_type == 'TIME':
                basefield = ',\n    ifnull({}.{},TIME(0,0,0)) as {}'.format(curtablealias,
                                                                            schema_item.name,
                                                                            fieldprefix +
                                                                            schema_item.name)
            elif schema_item.field_type == 'BYTES':
                basefield = ',\n    ifnull({}.{},b"\x00") as {}'.format(curtablealias,
                                                                        schema_item.name,
                                                                        fieldprefix +
                                                                        schema_item.name)
            elif schema_item.field_type == 'RECORD':
                aliasstack.append(curtablealias)
                fieldprefixstack.append(fieldprefix)
                fieldprefix = fieldprefix + schema_item.name
                if schema_item.mode == 'REPEATED':
                    oldalias = curtablealias
                    curtablealias = "A{}".format(basedata['aliasnum'])
                    basedata['aliasnum'] = basedata['aliasnum'] + 1

                    basedata['from'] = basedata['from'] + "\nLEFT JOIN UNNEST({}) as {}".format(
                        oldalias + "." + schema_item.name, curtablealias)

                else:
                    curtablealias = curtablealias + "." + schema_item.name
                recurse_diff_base(schema_item.fields, fieldprefix, curtablealias)
                curtablealias = aliasstack.pop()
                fieldprefix = fieldprefixstack.pop()
                continue
            update_only = False
            for fndi in update_only_fields:
                if schema_item.name == fndi:
                    update_only = True
                    break
            if update_only:
                fields_update_only.append(fieldprefix + schema_item.name)
            else:
                fields4diff.append(fieldprefix + schema_item.name)
            basedata['select'] = basedata['select'] + basefield
        return

    recurse_diff_base(schema, fieldprefix, curtablealias)
    views.append({"name": basediffview, "query": basedata['select'] + basedata['from'],
                  "description": "View used as basis for diffview:" + description})
    refbasediffview = "{}.{}.{}".format(project, dataset, basediffview)

    # Now fields4 diff has field sto compare fieldsnot4diff appear in select but are not compared.
    # basic logic is like below
    #
    # select action (a case statement but "Added","Deleted","Sames")
    # origfield,
    # lastfield,
    # if origfield != lastfield diff = 1 else diff = 0
    # from diffbaseview as orig with select of orig timestamp
    # from diffbaseview as later with select of later timestamp
    # This template logic is then changed for each interval to actually generate concrete views

    diffviewselectclause = """#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,"""
    diffieldclause = ""
    diffcaseclause = ""
    diffwhereclause = ""
    diffviewfromclause = """
  FROM (SELECT
     *
  FROM
    `{0}`
  WHERE
    scantime = (
    SELECT
      MAX({1})
    FROM
      `{2}.{3}.{4}`
    WHERE
      {1} < (
      SELECT
        MAX({1})
      FROM
        `{2}.{3}.{4}`)
      AND
      {1} < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL %interval%) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `{0}`
  WHERE
    scantime =(
    SELECT
      MAX({1})
    FROM
      `{2}.{3}.{4}` )) l
ON
""".format(refbasediffview, time_expr, project, dataset, table)

    for f4i in fields4diff:
        diffieldclause = diffieldclause + \
                         ",\n    o.{} as orig{},\n    l.{} as later{},\n    case " \
                         "when o.{} = l.{} " \
                         "then 0 else 1 end as diff{}".format(
                             f4i, f4i, f4i, f4i, f4i, f4i, f4i)
        if diffcaseclause == "":
            diffcaseclause = """
    CASE
    WHEN o.{} IS NULL THEN 'Added'
    WHEN l.{} IS NULL THEN 'Deleted'
    WHEN o.{} = l.{} """.format(f4i, f4i, f4i, f4i)
        else:
            diffcaseclause = diffcaseclause + "AND o.{} = l.{} ".format(f4i, f4i)
        if diffwhereclause == "":
            diffwhereclause = "    l.{} = o.{}".format(f4i, f4i)
        else:
            diffwhereclause = diffwhereclause + "\n    AND l.{}=o.{}".format(f4i, f4i)

    for f4i in fields_update_only:
        diffieldclause = diffieldclause + \
                         ",\n    o.{} as orig{},\n    l.{} as later{},\n    case " \
                         "" \
                         "when o.{} = l.{} " \
                         "then 0 else 1 end as diff{}".format(
                             f4i, f4i, f4i, f4i, f4i, f4i, f4i)
        diffcaseclause = diffcaseclause + "AND o.{} = l.{} ".format(f4i, f4i)

    diffcaseclause = diffcaseclause + """THEN 'Same'
    ELSE 'Updated'
  END AS action"""

    for intervali in intervals:
        for keyi in intervali:
            viewname = table + "diff" + keyi
            viewdescription = "Diff of {} of underlying table {} description: {}".format(
                keyi,
                table,
                description)
            views.append({"name": viewname,
                          "query": diffviewselectclause + diffcaseclause + diffieldclause +
                                   diffviewfromclause.replace(
                                       "%interval%", intervali[keyi]) + diffwhereclause,
                          "description": viewdescription})

    ## look for id in top level fields if exists create first seen and last seen views
    for i in schema:
        if i.name == "id":
            fsv = FSLST.format(project, dataset, table, "firstSeenTime", time_expr)
            fsd = FSLSTDT.format("first", project, dataset, table)
            lsv = FSLST.format(project, dataset, table, "lastSeenTime", time_expr)
            lsd = FSLSTDT.format("last", project, dataset, table)
            views.append({"name": table + "fs",
                          "query": fsv, "description": fsd})
            views.append({"name": table + "ls",
                          "query": lsv, "description": lsd})
            break

    return views


def evolve_schema(insertobj, table, client, bigquery, logger=None):
    """

    :param insertobj: json object that represents schema expected
    :param table: a table object from python api thats been git through client.get_table
    :param client: a big query client object
    :param bigquery: big query service as created with google discovery discovery.build(
    "bigquery","v2")
    :param logger: a google logger class
    :return: evolved True or False
    """

    schema = list(table.schema)
    tablechange = False

    evolved = match_and_addtoschema(insertobj, schema)

    if evolved:
        if logger is not None:
            logger.warning(
                u"Evolving schema as new field(s) on {}:{}.{} views with * will need "
                u"reapplying".format(
                    table.project, table.dataset_id, table.table_id))

        treq = bigquery.tables().get(projectId=table.project, datasetId=table.dataset_id,
                                     tableId=table.table_id)
        table_data = treq.execute()
        oschema = table_data.get('schema')
        tablechange, pschema = recurse_and_add_to_schema(schema, oschema['fields'])
        update = {'schema': {"fields": pschema}}
        preq = bigquery.tables().patch(projectId=table.project, datasetId=table.dataset_id,
                                       tableId=table.table_id,
                                       body=update)
        preq.execute()
        client.get_table(table)
        # table.reload()

    return evolved


def create_default_bq_resources(template, basename, project, dataset, location):
    """

    :param template: a template json object to create a big query schema for
    :param basename: a base name of the table to create that will also be used as a basis for views
    :param project: the project to create resources in
    :param dataset: the datasets to create them in
    :param location: The locatin
    :return: a list of big query table resources as dicionaries that can be passe dto code
    genearteor or used in rest
    calls
    """
    resourcelist = []
    table = {
        "type": "TABLE",
        "location": location,
        "tableReference": {
            "projectId": project,
            "datasetId": dataset,
            "tableId": basename
        },
        "timePartitioning": {
            "type": "DAY",
            "expirationMs": "94608000000"
        },
        "schema": {}
    }
    table["schema"]["fields"] = get_bq_schema_from_json_repr(template)
    resourcelist.append(table)
    views = gen_diff_views(project,
                           dataset,
                           basename,
                           create_schema(template))
    table = {
        "type": "VIEW",
        "tableReference": {
            "projectId": project,
            "datasetId": dataset,
            "tableId": "{}head".format(basename)
        },
        "view": {
            "query": HEADVIEW.format(project, dataset, basename),
            "useLegacySql": False

        }
    }
    resourcelist.append(table)
    for view_item in views:
        table = {
            "type": "VIEW",
            "tableReference": {
                "projectId": project,
                "datasetId": dataset,
                "tableId": view_item["name"]
            },
            "view": {
                "query": view_item["query"],
                "useLegacySql": False

            }
        }
        resourcelist.append(table)
    return resourcelist


class ViewCompiler(object):
    def __init__(self):
        self.view_depth_optimiser = []

    def compile(self, dataset, name, sql):

        standard_sql = True
        compiled_sql = sql
        prefix = ""
        if sql.strip().lower().find(u"#standardsql") == 0:
            prefix = u"#standardSQL\n"
        else:
            standard_sql = False
            prefix = u"#legacySQL\n"

        # get rid of nested comments as they can break this even if in a string in a query
        prefix = prefix + r"""
-- ===================================================================================
-- 
--                             ViewCompresser Output
--     
--                      \/\/\/\/\/\/Original SQL Below\/\/\/\/
"""
        for line in sql.splitlines():
            prefix = prefix + "-- " + line + "\n"
        prefix = prefix + r"""
-- 
--                      /\/\/\/\/\/\Original SQL Above/\/\/\/\
--                      
--                            Compiled SQL below
-- ===================================================================================
"""
        for i in self.view_depth_optimiser:
            # relaces a table or view name with sql
            if not standard_sql:
                compiled_sql = compiled_sql.replace(
                    "[" + i + "]",
                    "( /* flattened view [-" + i + "-]*/ " + self.view_depth_optimiser[i][
                        'unnested'] + ")")
            else:
                compiled_sql = compiled_sql.replace(
                    "`" + i.replace(':', '.') + "`",
                    "( /* flattened view `-" + i + "-`*/ " + self.view_depth_optimiser[i][
                        'unnested'] + ")")

        self.view_depth_optimiser[dataset.project + ":" + dataset.dataset_id + "." + name] = {
            "raw": sql,
            "unnested": compiled_sql}

        # look to keep queriesbelow maximumsize
        if len(prefix + compiled_sql) > 256000:
            # strip out comment
            if standard_sql:
                prefix = "#standardSQL\n"
            else:
                prefix = "#legacySQL\n"
            # if still too big strip out other comments
            # and extra space
            if len(prefix + compiled_sql) > 256000:
                nsql = ''
                for line in compiled_sql.split("\n").trim():
                    # if not a comment
                    if line[:2] != "--":
                        ' '.join(line.split())
                        nsql = nsql + "\n" + line
                compiled_sql = nsql

                # if still too big go back to original sql stripped
                if len(prefix + compiled_sql) > 256000:
                    if len(sql) > 256000:
                        nsql = ''
                        for line in origsql.split("\n").trim():
                            # if not a comment
                            if line[:1] != "#":
                                ' '.join(line.split())
                                nsql = nsql + "\n" + line
                                compiled_sql = nsql
                    else:
                        compiled_sql = sql

        return prefix + compiled_sql


def compute_region_equals_bqregion(compute_region, bq_region):
    if compute_region == bq_region or compute_region.lower() == bq_region.lower():
        bq_compute_region = bq_region.lower()
    else:
        bq_compute_region = MAPBQREGION2KMSREGION.get(bq_region, bq_region.lower())
    return compute_region.lower() == bq_compute_region


def run_query(client, query, logger, desctext="", location=None, max_results=10000,
              callback_on_complete=None, labels=None):
    """
    Runa big query query and yield on each row returned as a generator
    :param client: The BQ client to use to run the query
    :param query: The query text assumed standardsql unless starts with #legcaySQL
    :param desctext: Some descriptive text to put out if in debug mode
    :return: nothing
    """
    use_legacy_sql = False
    if query.lower().find('#legacysql') == 0:
        use_legacy_sql = True

    job_config = bigquery.QueryJobConfig()
    job_config.maximum_billing_tier = 10
    job_config.use_legacy_sql = use_legacy_sql
    if labels is not None:
        job_config.labels = labels

    query_job = client.query(query, job_config=job_config, location=location)

    pretty_printer = pprint.PrettyPrinter(indent=4)
    results = False
    while True:
        query_job.reload()  # Refreshes the state via a GET request.

        if query_job.state == 'DONE':
            if query_job.error_result:
                errtext = u"Query error {}{}".format(pretty_printer.pformat(query_job.error_result),
                                                     pretty_printer.pformat(query_job.errors))
                logger.error(errtext, exc_info=True)
                raise BQQueryError(
                    query, desctext, errtext)
            else:
                results = True
                break

    if results:
        # query_results = query_job.results()

        # Drain the query results by requesting
        # a page at a time.
        # page_token = None

        for irow in query_job.result():
            yield irow

        if callback_on_complete is not None and callable(callback_on_complete):
            callback_on_complete(query_job)

    return

class ExportImportType(object):
    """
    Class that calculate the export import types that are best to use to copy the table
    passed in initialiser across region.
    """
    def __init__(self,srctable,dsttable=None):
        """
        Construct an ExportImportType around  a tbale that describes best format to copy this table across region
        how to compress
        :param srctable:
        """
        assert isinstance(srctable,bigquery.Table), "Export Import Type MUST be constructed with a bigquery.Table object"
        assert dsttable is None or isinstance(dsttable, bigquery.Table), "Export Import dsttabl Type MUST be constructed with a bigquery.Table object or None"

        if dsttable is None:
            self.__table = srctable
        else:
            self.__table = dsttable

        # detect if any GEOGRAPHY or DATETIME fields
        def _detect_non_avro_types(schema):
            for field in schema:
                if field.field_type == 'GEOGRAPHY' or field.field_type == "DATETIME":
                    return True
                if field.field_type == "RECORD":
                    if _detect_non_avro_types(list(field.fields)):
                        return True
            return False

        self.__destination_format = bigquery.job.DestinationFormat.AVRO
        if _detect_non_avro_types(list(srctable.schema)):
            self.__destination_format = bigquery.job.DestinationFormat.NEWLINE_DELIMITED_JSON

    @property
    def destination_format(self):
        """
        The destination format to use for exports for ths table when copying across regions
        :return: a bigquery.job.DestinationFormat enumerator
        """
        return self.__destination_format

    @property
    def source_format(self):
        """
        The source format to use to load this table in destnation region
        :return:  a bigquery.job.SourceFormat enumerator that matches the prefferd export format
        """
        # only support the exports that are possible
        if self.destination_format == bigquery.job.DestinationFormat.AVRO:
            return bigquery.job.SourceFormat.AVRO
        if self.destination_format == bigquery.job.DestinationFormat.NEWLINE_DELIMITED_JSON:
            return bigquery.job.SourceFormat.NEWLINE_DELIMITED_JSON
        if self.destination_format == bigquery.job.DestinationFormat.CSV:
            return bigquery.job.SourceFormat.CSV

    @property
    def compression_format(self):
        """
        The calculated compression type to use based on supported format
        :return:  one of bigquery.job.Compression enumerators or None
        """
        if self.destination_format == bigquery.job.DestinationFormat.AVRO:
            return bigquery.job.Compression.DEFLATE
        return bigquery.job.Compression.GZIP

    @property
    def schema(self):
        """
        The target schema so if needed on load can be obtained from same object
        :return:
        """
        return self.__table.schema

    @property
    def encryption_configuration(self):
        return self.__table.encryption_configuration


class DefaultBQSyncDriver(object):
    """ This class provides mechanical input to bqsync functions"""
    threadLocal = threading.local()

    def __init__(self, srcproject, srcdataset, dstdataset, dstproject=None,
                 srcbucket=None, dstbucket=None, remove_deleted_tables=True,
                 copy_data=True,
                 copy_types=["TABLE", "VIEW", "ROUTINE", "MODEL"],
                 check_depth=-1,
                 copy_access=True,
                 table_view_filter=[".*"],
                 table_or_views_to_exclude=[],
                 latest_date=None,
                 days_before_latest_day=None,
                 day_partition_deep_check=False,
                 analysis_project=None):
        """
        Constructor for base copy driver all other drivers should inherit from this
        :param srcproject: The project that is the source for the copy (note all actions are done
        inc ontext of source project)
        :param srcdataset: The source dataset
        :param dstdataset: The destination dataset
        :param dstproject: The source project if None assumed to be source project
        :param srcbucket:  The source bucket when copying cross region data is extracted to this
        bucket rewritten to destination bucket
        :param dstbucket: The destination bucket where data is loaded from
        :param remove_deleted_tables: If table exists in destination but not in source should it
        be deleted
        :param copy_data: Copy data or just do schema
        :param copy_types: Copy object types i.e. TABLE,VIEW,ROUTINE,MODEL
        """
        if dstproject is None:
            dstproject = srcproject

        self._remove_deleted_tables = remove_deleted_tables

        # check copy makes some basic sense
        assert srcproject != dstproject or srcdataset != dstdataset, "Source and destination " \
                                                                     "datasets cannot be the same"
        assert latest_date is None or isinstance(latest_date, datetime)

        self._source_project = srcproject
        self._source_dataset = srcdataset
        self._destination_project = dstproject
        self._destination_dataset = dstdataset
        self._copy_data = copy_data
        self._http = None
        self.__copy_q = None
        self.__schema_q = None
        self.__jobs = []
        self.__copy_types = copy_types
        self.reset_stats()
        self.__logger = logging
        self.__check_depth = check_depth
        self.__copy_access = copy_access
        self.__table_view_filter = table_view_filter
        self.__table_or_views_to_exclude = table_or_views_to_exclude
        self.__re_table_view_filter = []
        self.__re_table_or_views_to_exclude = []
        self.__base_predicates = []
        self.__day_partition_deep_check = day_partition_deep_check
        self.__analysisproject = self._destination_project
        if analysis_project is not None:
            self.__analysisproject = analysis_project

        if days_before_latest_day is not None:
            if latest_date is None:
                end_date = "TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY)"
            else:
                end_date = "TIMESTAMP('{}')".format(latest_date.strftime("%Y-%m-%d"))
            self.__base_predicates.append(
                "{{retpartition}} BETWEEN TIMESTAMP_SUB({end_date}, INTERVAL {"
                "days_before_latest_day} * 24 HOUR) AND {end_date}".format(
                    end_date=end_date,
                    days_before_latest_day=days_before_latest_day))

        # now check that from a service the copy makes sense
        assert dataset_exists(self.source_client, self.source_client.dataset(
            self.source_dataset)), "Source dataset does not exist %r" % self.source_dataset
        assert dataset_exists(self.destination_client,
                              self.destination_client.dataset(
                                  self.destination_dataset)), "Destination dataset does not " \
                                                              "exists %r" % self.destination_dataset

        # figure out if cross region copy if within region copies optimised to happen in big query
        # if cross region buckets need to exist to support copy and they need to be in same region
        source_dataset_impl = self.source_dataset_impl
        destination_dataset_impl = self.destination_dataset_impl
        self._same_region = source_dataset_impl.location == destination_dataset_impl.location
        self._source_location = source_dataset_impl.location
        self._destination_location = destination_dataset_impl.location

        # if not same region where are the buckets for copying
        if not self.same_region:
            assert srcbucket is not None, "Being asked to copy datasets across region but no " \
                                          "source bucket is defined these must be in same region " \
                                          "as source dataset"
            assert isinstance(srcbucket,
                              six.string_types), "Being asked to copy datasets across region but " \
                                                 "no " \
                                                 "" \
                                                 "" \
                                                 "source bucket is not a string"
            self._source_bucket = srcbucket
            assert dstbucket is not None, "Being asked to copy datasets across region but no " \
                                          "destination bucket is defined these must be in same " \
                                          "region " \
                                          "as destination dataset"
            assert isinstance(dstbucket,
                              six.string_types), "Being asked to copy datasets across region but " \
                                                 "destination bucket is not a string"
            self._destination_bucket = dstbucket
            client = storage.Client(project=self.source_project)
            src_bucket = client.get_bucket(self.source_bucket)
            assert compute_region_equals_bqregion(src_bucket.location,
                                                  source_dataset_impl.location), "Source bucket " \
                                                                                 "location is not " \
                                                                                 "" \
                                                                                 "" \
                                                                                 "same as source " \
                                                                                 "dataset location"
            dst_bucket = client.get_bucket(self.destination_bucket)
            assert compute_region_equals_bqregion(dst_bucket.location,
                                                  destination_dataset_impl.location), \
                "Destination bucket location is not same as destination dataset location"

    def base_predicates(self, retpartition):
        actual_basepredicates = []
        for predicate in self.__base_predicates:
            actual_basepredicates.append(predicate.format(retpartition=retpartition))
        return actual_basepredicates

    def comparison_predicates(self, table_name, retpartition="_PARTITIONTIME"):
        return self.base_predicates(retpartition)

    def istableincluded(self, table_name):
        """
        This method when passed a table_name returns true if it should be processed in a copy action
        :param table_name:
        :return: boolean True then it should be include False then no
        """
        if len(self.__re_table_view_filter) == 0:
            for filter in self.__table_view_filter:
                self.__re_table_view_filter.append(re.compile(filter))
            for filter in self.__table_or_views_to_exclude:
                self.__re_table_or_views_to_exclude.append(re.compile(filter))

        result = False

        for regexp2check in self.__re_table_view_filter:
            if regexp2check.search(table_name):
                result = True
                break

        if result:
            for regexp2check in self.__re_table_or_views_to_exclude:
                if regexp2check.search(table_name):
                    result = False
                    break

        return result

    def reset_stats(self):
        self.__bytes_synced = 0
        self.__rows_synced = 0
        self.__bytes_avoided = 0
        self.__rows_avoided = 0
        self.__tables_synced = 0
        self.__views_synced = 0
        self.__routines_synced = 0
        self.__routines_failed_sync = 0
        self.__routines_avoided = 0
        self.__models_synced = 0
        self.__models_failed_sync = 0
        self.__models_avoided = 0
        self.__views_failed_sync = 0
        self.__tables_failed_sync = 0
        self.__tables_avoided = 0
        self.__view_avoided = 0
        self.__extract_fails = 0
        self.__load_fails = 0
        self.__copy_fails = 0
        self.__query_cache_hits = 0
        self.__total_bytes_processed = 0
        self.__total_bytes_billed = 0
        self.__start_time = None
        self.__end_time = None
        self.__load_input_file_bytes = 0
        self.__load_input_files = 0
        self.__load_output_bytes = 0
        self.__blob_rewrite_retried_exceptions = 0
        self.__blob_rewrite_unretryable_exceptions = 0

    @property
    def blob_rewrite_retried_exceptions(self):
        return self.__blob_rewrite_retried_exceptions

    @property
    def blob_rewrite_unretryable_exceptions(self):
        return self.__blob_rewrite_unretryable_exceptions

    def increment_blob_rewrite_retried_exceptions(self):
        self.__blob_rewrite_retried_exceptions += 1

    def increment_blob_rewrite_unretryable_exceptions(self):
        self.__blob_rewrite_unretryable_exceptions += 1

    @property
    def models_synced(self):
        return self.__models_synced

    def increament_models_synced(self):
        self.__models_synced += 1

    @property
    def models_failed_sync(self):
        return self.__models_failed_sync

    def increment_models_failed_sync(self):
        self.__models_failed_sync += 1

    @property
    def models_avoided(self):
        return self.__models_avoided

    def increment_models_avoided(self):
        self.__models_avoided += 1

    @property
    def routines_synced(self):
        return self.__routines_synced

    def increment_routines_synced(self):
        self.__routines_synced += 1

    def increment_routines_avoided(self):
        self.__routines_avoided += 1

    @property
    def routines_failed_sync(self):
        return self.__routines_failed_sync

    @property
    def routines_avoided(self):
        return self.__routines_avoided

    def increment_rows_avoided(self):
        self.__routines_avoided += 1

    def increment_routines_failed_sync(self):
        self.__routines_failed_sync += 1

    @property
    def bytes_copied_across_region(self):
        return self.__load_input_file_bytes

    @property
    def files_copied_across_region(self):
        return self.__load_input_files

    @property
    def bytes_copied(self):
        return self.__load_output_bytes

    def increment_load_input_file_bytes(self, value):
        self.__load_input_file_bytes += value

    def increment_load_input_files(self, value):
        self.__load_input_files += value

    def increment_load_output_bytes(self, value):
        self.__load_output_bytes += value

    @property
    def start_time(self):
        return self.__start_time

    @property
    def end_time(self):
        if self.__end_time is None and self.__start_time is not None:
            return datetime.utcnow()
        return self.__end_time

    @property
    def sync_time_seconds(self):
        if self.__start_time is None:
            return None
        return (self.__end_time - self.__start_time).seconds

    def start_sync(self):
        self.__start_time = datetime.utcnow()

    def end_sync(self):
        self.__end_time = datetime.utcnow()

    @property
    def query_cache_hits(self):
        return self.__query_cache_hits

    def increment_cache_hits(self):
        self.__query_cache_hits += 1

    @property
    def total_bytes_processed(self):
        return self.__total_bytes_processed

    def increment_total_bytes_processed(self, total_bytes_processed):
        self.__total_bytes_processed += total_bytes_processed

    @property
    def total_bytes_billed(self):
        return self.__total_bytes_processed

    def increment_total_bytes_billed(self, total_bytes_billed):
        self.__total_bytes_billed += total_bytes_billed

    @property
    def copy_fails(self):
        return self.__copy_fails

    @property
    def copy_access(self):
        return self.__copy_access

    @copy_access.setter
    def copy_access(self, value):
        self.__copy_access = value

    def increment_copy_fails(self):
        self.__copy_fails += 1

    @property
    def load_fails(self):
        return self.__load_fails

    def increment_load_fails(self):
        self.__load_fails += 1

    @property
    def extract_fails(self):
        return self.__extract_fails

    def increment_extract_fails(self):
        self.__extract_fails += 1

    @property
    def check_depth(self):
        return self.__check_depth

    @check_depth.setter
    def check_depth(self, value):
        self.__check_depth = value

    @property
    def views_failed_sync(self):
        return self.__views_failed_sync

    def increment_views_failed_sync(self):
        self.__views_failed_sync += 1

    @property
    def tables_failed_sync(self):
        return self.__tables_failed_sync

    def increment_tables_failed_sync(self):
        self.__tables_failed_sync += 1

    @property
    def tables_avoided(self):
        return self.__tables_avoided

    def increment_tables_avoided(self):
        self.__tables_avoided += 1

    @property
    def view_avoided(self):
        return self.__view_avoided

    def increment_view_avoided(self):
        self.__view_avoided += 1

    @property
    def bytes_synced(self):
        return self.__bytes_synced

    @property
    def copy_types(self):
        return self.__copy_types

    def add_bytes_synced(self, bytes):
        self.__bytes_synced += bytes

    def update_job_stats(self, job):
        """
        Given a big query job figure out what stats to process
        :param job:
        :return: None
        """
        if isinstance(job, bigquery.QueryJob):
            if job.cache_hit:
                self.increment_cache_hits
            self.increment_total_bytes_billed(job.total_bytes_billed)
            self.increment_total_bytes_processed(job.total_bytes_processed)

        if isinstance(job, bigquery.CopyJob):
            if job.error_result is not None:
                self.increment_copy_fails()

        if isinstance(job, bigquery.LoadJob):
            if job.error_result:
                self.increment_load_fails()
            else:
                self.increment_load_input_files(job.input_files)
                self.increment_load_input_file_bytes(job.input_file_bytes)
                self.increment_load_output_bytes(job.output_bytes)

    @property
    def rows_synced(self):
        # as time can be different between these assume avoided is always more accurae
        if self.rows_avoided > self.__rows_synced:
            return self.rows_avoided
        return self.__rows_synced

    def add_rows_synced(self, rows):
        self.__rows_synced += rows

    @property
    def bytes_avoided(self):
        return self.__bytes_avoided

    def add_bytes_avoided(self, bytes):
        self.__bytes_avoided += bytes

    @property
    def rows_avoided(self):
        return self.__rows_avoided

    def add_rows_avoided(self, rows):
        self.__rows_avoided += rows

    @property
    def tables_synced(self):
        return self.__tables_synced

    @property
    def views_synced(self):
        return self.__views_synced

    def increment_tables_synced(self):
        self.__tables_synced += 1

    def increment_views_synced(self):
        self.__views_synced += 1

    @property
    def copy_q(self):
        return self.__copy_q

    @copy_q.setter
    def copy_q(self, value):
        self.__copy_q = value

    @property
    def schema_q(self):
        return self.__schema_q

    @schema_q.setter
    def schema_q(self, value):
        self.__schema_q = value

    @property
    def source_location(self):
        return self._source_location

    @property
    def destination_location(self):
        return self._destination_location

    @property
    def source_bucket(self):
        return self._source_bucket

    @property
    def destination_bucket(self):
        return self._destination_bucket

    @property
    def same_region(self):
        return self._same_region

    @property
    def source_dataset_impl(self):
        source_datasetref = self.source_client.dataset(self.source_dataset)
        return self.source_client.get_dataset(source_datasetref)

    @property
    def destination_dataset_impl(self):
        destination_datasetref = self.destination_client.dataset(self.destination_dataset)
        return self.destination_client.get_dataset(destination_datasetref)

    @property
    def query_client(self):
        """
        Returns the client to be charged for analysis of comparison could be
        source could be destination could be another.
        By default it is the destination but can be overriden by passing a target project
        :return: A big query client for the project to be charged
        """
        warnings.filterwarnings("ignore",
                                "Your application has authenticated using end user credentials")
        """
        Obtains a source client in the current thread only constructs a client once per thread
        :return:
        """
        source_client = getattr(
            DefaultBQSyncDriver.threadLocal, self.__analysisproject, None)
        if source_client is None:
            setattr(DefaultBQSyncDriver.threadLocal, self.__analysisproject,
                    bigquery.Client(project=self.__analysisproject, _http=self.http))
        return getattr(
            DefaultBQSyncDriver.threadLocal, self.__analysisproject, None)

    @property
    def source_client(self):
        warnings.filterwarnings("ignore",
                                "Your application has authenticated using end user credentials")
        """
        Obtains a source client in the current thread only constructs a client once per thread
        :return:
        """
        source_client = getattr(
            DefaultBQSyncDriver.threadLocal, self.source_project + self._source_dataset, None)
        if source_client is None:
            setattr(DefaultBQSyncDriver.threadLocal, self.source_project + self._source_dataset,
                    bigquery.Client(project=self.source_project, _http=self.http))
        return getattr(
            DefaultBQSyncDriver.threadLocal, self.source_project + self._source_dataset, None)

    @property
    def http(self):
        """
        Allow override of http transport per client
        usefule for proxy handlng but should be handled by sub-classes default is do nothing
        :return:
        """
        self._http

    @property
    def destination_client(self):
        """
        Obtains a s destination client in current thread only constructs a client once per thread
        :return:
        """
        source_client = getattr(
            DefaultBQSyncDriver.threadLocal, self.destination_project + self.destination_dataset,
            None)
        if source_client is None:
            setattr(DefaultBQSyncDriver.threadLocal,
                    self.destination_project + self.destination_dataset,
                    bigquery.Client(project=self.destination_project, _http=self.http))
        return getattr(
            DefaultBQSyncDriver.threadLocal, self.destination_project + self.destination_dataset,
            None)

    def export_import_format_supported(self,srctable,dsttable=None):
        """ Calculates a suitable export import type based upon schema
        default mecahnism is to use AVRO and SNAPPY as parallel and fast
        If though a schema type notsupported by AVRO fall back to jsonnl and gzip
        """
        return ExportImportType(srctable,dsttable)

    @property
    def source_project(self):
        return self._source_project

    @property
    def source_dataset(self):
        return self._source_dataset

    @property
    def jobs(self):
        return self.__jobs

    def add_job(self, job):
        self.__jobs.append(job)

    @property
    def destination_project(self):
        return self._destination_project

    @property
    def destination_dataset(self):
        return self._destination_dataset

    @property
    def remove_deleted_tables(self):
        return self._remove_deleted_tables

    def day_partition_deep_check(self):
        """

        :return: True if should check rows and bytes counts False if notrequired
        """
        return self.__day_partition_deep_check

    def extra_dp_compare_functions(self, table):
        """
        Function to allow record comparison checks for a day partition
        These shoul dbe aggregates for the partition i..e max, min, avg
        Override when row count if not sufficient. Row count works for
        tables where rows only added non deleted or removed
        :return: a comma seperated extension of functions
        """
        default = ""
        retpartition_time = "_PARTITIONTIME"

        if getattr(table, "time_partitioning", None) and table.time_partitioning.field is not None:
            retpartition_time = "TIMESTAMP({})".format(table.time_partitioning.field)

        SCHEMA = list(table.schema)

        # emulate nonlocal variables this works in python 2.7 and python 3.6
        aliasdict = {"alias": "", "extrajoinpredicates": ""}

        """
        
        Use FRAM_FINGERPRINT as hash of each value and then summed
        basically a merkel function
        
        """

        def add_data_check(SCHEMA, prefix=None, depth=0):

            if prefix is None:
                prefix = []
                # add base table alia
                prefix.append('zzz')

            expression_list = []

            # if we are beyond check depth exit
            if self.check_depth >= 0 and depth > self.check_depth:
                return expression_list

            for field in SCHEMA:
                prefix.append(field.name)
                if field.mode != "REPEATED":
                    if self.check_depth >= 0 or \
                            (self.check_depth >= -1 and (
                                    re.search("update.*time", field.name.lower()) or \
                                    re.search("modifi.*time", field.name.lower()) or \
                                    re.search("version", field.name.lower()) or \
                                    re.search("creat.*time", field.name.lower()))):

                        if field.field_type == "STRING":
                            expression_list.append("IFNULL(`{0}`,'')".format("`.`".join(prefix)))
                        elif field.field_type == "TIMESTAMP":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,TIMESTAMP('1970-01-01')) AS STRING)".format(
                                    "`.`".join(prefix)))
                        elif field.field_type == "INTEGER" or field.field_type == "INT64":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,0) AS STRING)".format("`.`".join(prefix)))
                        elif field.field_type == "FLOAT" or field.field_type == "FLOAT64" or \
                                field.field_type == "NUMERIC":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,0.0) AS STRING)".format("`.`".join(prefix)))
                        elif field.field_type == "BOOL" or field.field_type == "BOOLEAN":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,false) AS STRING)".format("`.`".join(prefix)))
                        elif field.field_type == "BYTES":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,'') AS STRING)".format("`.`".join(prefix)))
                    if field.field_type == "RECORD":
                        SSCHEMA = list(field.fields)
                        expression_list.extend(add_data_check(SSCHEMA, prefix=prefix, depth=depth))
                else:
                    if field.field_type != "RECORD" and (self.check_depth >= 0 or \
                                                         (self.check_depth >= -1 and (
                                                                 re.search("update.*time",
                                                                           field.name.lower()) or \
                                                                 re.search("modifi.*time",
                                                                           field.name.lower()) or \
                                                                 re.search("version",
                                                                           field.name.lower()) or \
                                                                 re.search("creat.*time",
                                                                           field.name.lower())))):
                        # add the unnestof repeated base type can use own field name
                        fieldname = "{}{}".format(aliasdict["alias"], field.name)
                        aliasdict["extrajoinpredicates"] = """{}
LEFT JOIN UNNEST(`{}`) AS `{}`""".format(aliasdict["extrajoinpredicates"],
                                         field.name,
                                         fieldname)
                        if field.field_type == "STRING":
                            expression_list.append("IFNULL(`{0}`,'')".format(fieldname))
                        elif field.field_type == "TIMESTAMP":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,TIMESTAMP('1970-01-01')) AS STRING)".format(
                                    fieldname))
                        elif field.field_type == "INTEGER" or field.field_type == "INT64":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,0) AS STRING)".format(fieldname))
                        elif field.field_type == "FLOAT" or field.field_type == "FLOAT64" or \
                                field.field_type == "NUMERIC":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,0.0) AS STRING)".format(fieldname))
                        elif field.field_type == "BOOL" or field.field_type == "BOOLEAN":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,false) AS STRING)".format(fieldname))
                        elif field.field_type == "BYTES":
                            expression_list.append(
                                "CAST(IFNULL(`{0}`,'') AS STRING)".format(fieldname))
                    if field.field_type == "RECORD":
                        # if we want to go deeper in checking
                        if depth < self.check_depth:

                            # need to unnest as repeated
                            if aliasdict["alias"] == "":
                                # uses this to name space from real columns
                                # bit hopeful
                                aliasdict["alias"] = "zzz"

                            aliasdict["alias"] = aliasdict["alias"] + "z"

                            # so need to rest prefix for this record
                            newprefix = []
                            newprefix.append(aliasdict["alias"])

                            # add the unnest
                            aliasdict["extrajoinpredicates"] = """{}
LEFT JOIN UNNEST(`{}`) AS {}""".format(aliasdict["extrajoinpredicates"], "`.`".join(prefix),
                                       aliasdict["alias"])

                            # add the fields
                            expression_list.extend(
                                add_data_check(SSCHEMA, prefix=newprefix, depth=depth + 1))
                prefix.pop()
            return expression_list

        expression_list = add_data_check(SCHEMA)

        if len(expression_list) > 0:
            # algorithm to compare sets
            # java uses overflowing sum but big query does not allow in sum
            # using average as scales sum and available as aggregate
            # split top end and bottom end of hash
            # that way we maximise bits and fidelity
            # and
            default = """,
    AVG(FARM_FINGERPRINT(CONCAT({0})) &  0x0000000FFFFFFFF) as avgFingerprintLB,
    AVG((FARM_FINGERPRINT(CONCAT({0})) & 0xFFFFFFF00000000) >> 32) as avgFingerprintHB,""".format(
                ",".join(expression_list))

        predicates = self.comparison_predicates(table.table_id, retpartition_time)
        if len(predicates) > 0:
            aliasdict["extrajoinpredicates"] = """{}
WHERE ({})""".format(aliasdict["extrajoinpredicates"], ") AND (".join(predicates))

        return retpartition_time, default, aliasdict["extrajoinpredicates"]

    @property
    def copy_data(self):
        """
        True if to copy data not just structure
        False just keeps structure in sync
        :return:
        """
        return self._copy_data

    def table_data_change(self, srctable, dsttable):
        """
        Method to allow customisation of detecting if table differs
        default method is check rows, numberofbytes and last modified time
        this exists to allow something like a query to do comparison
        if you want to force a copy this should retrn True
        :param srctable:
        :param dsttable:
        :return:
        """

        return False

    def fault_barrier(self, function, *args):
        """
        A fault barrie here to ensure functions called in thread
        do n9t exit prematurely
        :param function: A function to call
        :param args: The functions arguments
        :return:
        """
        try:
            function(*args)
        except Exception as e:
            pretty_printer = pprint.PrettyPrinter()
            self.get_logger().exception(
                "Exception calling function {} args {}".format(function.__name__,
                                                               pretty_printer.pformat(args)))

    def update_source_view_definition(self, view_definition, use_standard_sql):
        view_definition = view_definition.replace(
            r'`{}.{}.'.format(self.source_project, self.source_dataset),
            "`{}.{}.".format(self.destination_project, self.destination_dataset))
        view_definition = view_definition.replace(
            r'[{}:{}.'.format(self.source_project, self.source_dataset),
            "[{}:{}.".format(self.destination_project, self.destination_dataset))
        # this should not be required but seems it is
        view_definition = view_definition.replace(
            r'[{}.{}.'.format(self.source_project, self.source_dataset),
            "[{}:{}.".format(self.destination_project, self.destination_dataset))

        return view_definition

    def calculate_target_cmek_config(self, encryption_config):
        assert isinstance(encryption_config,
                          bigquery.EncryptionConfiguration), \
                          " To recaclculate a new encryption " \
                          "config the original config has to be passed in and be of class " \
                          "bigquery.EncryptionConfig"
        # if destination dataset has default kms key already, just use the same
        if self.destination_dataset_impl.default_encryption_configuration is not None:
            return self.destination_dataset_impl.default_encryption_configuration
        # if a global key or same region we are good to go
        if self.same_region or encryption_config.kms_key_name.find(
                                    "/locations/global/") != -1:
            return encryption_config

        # if global key can still be used
        parts = encryption_config.kms_key_name.split("/")
        parts[3] = MAPBQREGION2KMSREGION.get(self.destination_location,
                                             self.destination_location.lower())

        return bigquery.encryption_configuration.EncryptionConfiguration(kms_key_name="/".join(parts))


    def copy_access_to_destination(self):
        # for those not created compare data structures
        # copy data
        # compare data
        # copy views
        # copy dataset permissions
        if self.copy_access:
            src_dataset = self.source_client.get_dataset(
                self.source_client.dataset(self.source_dataset))
            dst_dataset = self.destination_client.get_dataset(
                self.destination_client.dataset(self.destination_dataset))
            access_entries = src_dataset.access_entries
            dst_access_entries = []
            for access in access_entries:
                newaccess = access
                if access.role is None:
                    # if not copying views these will fail
                    if "VIEW" not in self.copy_types:
                        continue
                    newaccess = self.create_access_view(access.entity_id)
                dst_access_entries.append(newaccess)
            dst_dataset.access_entries = dst_access_entries

            fields = ["access_entries"]
            if dst_dataset.description != src_dataset.description:
                dst_dataset.description = src_dataset.description
                fields.append("description")

            if dst_dataset.friendly_name != src_dataset.friendly_name:
                dst_dataset.friendly_name = src_dataset.friendly_name
                fields.append("friendly_name")

            if dst_dataset.default_table_expiration_ms != src_dataset.default_table_expiration_ms:
                dst_dataset.default_table_expiration_ms = src_dataset.default_table_expiration_ms
                fields.append("default_table_expiration_ms")

            if getattr(dst_dataset, "default_partition_expiration_ms", None):
                if dst_dataset.default_partition_expiration_ms != \
                        src_dataset.default_partition_expiration_ms:
                    dst_dataset.default_partition_expiration_ms = \
                        src_dataset.default_partition_expiration_ms
                    fields.append("default_partition_expiration_ms")

            # compare 2 dictionaries that are simple key, value
            x = dst_dataset.labels
            y = src_dataset.labels

            # get shared key values
            shared_items = {k: x[k] for k in x if k in y and x[k] == y[k]}

            # must be same size and values if not set labels
            if len(dst_dataset.labels) != len(src_dataset.labels) or len(shared_items) != len(
                    src_dataset.labels):
                dst_dataset.labels = src_dataset.labels
                fields.append("labels")

            if getattr(dst_dataset, "default_encryption_configuration", None):
                if not (
                        src_dataset.default_encryption_configuration is None and
                        dst_dataset.default_encryption_configuration is None):
                    # if src_dataset.default_encryption_configuration is None:
                    #     dst_dataset.default_encryption_configuration = None
                    # else:
                    #     dst_dataset.default_encryption_configuration = \
                    #         self.calculate_target_cmek_config(
                    #             src_dataset.default_encryption_configuration)
                    
                    # equate dest kms config to src only if it's None
                    if dst_dataset.default_encryption_configuration is None:
                        dst_dataset.default_encryption_configuration = \
                            self.calculate_target_cmek_config(src_dataset.default_encryption_configuration)
                    fields.append("default_encryption_configuration")

            try:
                self.destination_client.update_dataset(dst_dataset, fields)
            except exceptions.Forbidden as e:
                self.logger.error(
                    "Unable to det permission on {}.{} dataset as Forbidden".format(
                        self.destination_project,
                        self.destination_dataset))
            except exceptions.BadRequest as e:
                self.logger.error(
                    "Unable to det permission on {}.{} dataset as BadRequest".format(
                        self.destination_project,
                        self.destination_dataset))

    def create_access_view(self, entity_id):
        """
        Convert an old view authorised view
        to a new one i.e. change project id

        :param entity_id:
        :return: a view {
        ...     'projectId': 'my-project',
        ...     'datasetId': 'my_dataset',
        ...     'tableId': 'my_table'
        ... }
        """
        if enity_id["projectId"] == self.source_project and enity_id[
            "datasetId"] == self.source_dataset:
            enity_id["projectId"] = self.destination_project
            entity_id["datasetId"] = self.destination_dataset

        return bigquery.AccessEntry(None, 'view', enity_id)

    @property
    def logger(self):
        return self.__logger

    @logger.setter
    def logger(self, alogger):
        self.__logger = alogger

    def get_logger(self):
        """
        Returns the python logger to use for logging errors and issues
        :return:
        """
        return self.__logger


class MultiBQSyncCoordinator(object):
    """
    Class to copy many datasets from one region to another and reset associated views
    """
    """
        States of sync job
    """
    NOTSTARTED = "NOTSTARTED"
    RUNNING = "RUNNING"
    FINSIHED = "FINSIHED"

    def __init__(self, srcproject_and_dataset_list, dstproject_and_dataset_list,
                 srcbucket=None, dstbucket=None, remove_deleted_tables=True,
                 copy_data=True,
                 copy_types=["TABLE", "VIEW", "ROUTINE", "MODEL"],
                 check_depth=-1,
                 copy_access=True,
                 table_view_filter=[".*"],
                 table_or_views_to_exclude=[],
                 latest_date=None,
                 days_before_latest_day=None,
                 day_partition_deep_check=False,
                 analysis_project=None,
                 cloud_logging_and_monitoring=False,
                 src_ref_project_datasets=[],
                 dst_ref_project_datasets=[]):
        assert len(srcproject_and_dataset_list) == len(
            dstproject_and_dataset_list), "Source and destination lists must be same length"
        assert len(
            srcproject_and_dataset_list) > 0, "Fro multi copy we need at least 1 source and 1 " \
                                              "destination"

        assert len(src_ref_project_datasets) == len(
            dst_ref_project_datasets), "referenced datsets dst_ref_project_datasets and " \
                                       "dst_ref_project_datasets MUST be same length"
        self.__src_ref_project_datasets = src_ref_project_datasets
        self.__dst_ref_project_datasets = dst_ref_project_datasets

        # create the underlying copy drivers to copy each dataset
        # the sequence of these should be any cross dataset views are created correctly
        self.__copy_drivers = []
        self.__check_depth = check_depth
        self.__cloud_logging_and_monitoring = cloud_logging_and_monitoring
        if self.cloud_logging_and_monitoring:
            # TODO: Add metrics
            pass

        # create a copy driver for each pair of source destinations
        # note assumption is a set will always be from same source location to destination location
        for itemnum, src_project_dataset in enumerate(srcproject_and_dataset_list):
            dst_project_dataset = dstproject_and_dataset_list[itemnum]
            copy_driver = MultiBQSyncDriver(src_project_dataset.split(".")[0],
                                            src_project_dataset.split(".")[1],
                                            dst_project_dataset.split(".")[1],
                                            dstproject=dst_project_dataset.split(".")[0],
                                            srcbucket=srcbucket, dstbucket=dstbucket,
                                            remove_deleted_tables=remove_deleted_tables,
                                            copy_data=copy_data,
                                            copy_types=copy_types,
                                            check_depth=self.__check_depth,
                                            coordinator=self,
                                            copy_access=copy_access,
                                            table_view_filter=table_view_filter,
                                            table_or_views_to_exclude=table_or_views_to_exclude,
                                            latest_date=latest_date,
                                            days_before_latest_day=days_before_latest_day,
                                            day_partition_deep_check=day_partition_deep_check,
                                            analysis_project=analysis_project)
            self.__copy_drivers.append(copy_driver)

    @property
    def cloud_logging_and_monitoring(self):
        return self.__cloud_logging_and_monitoring

    @property
    def cloud_monitoring_client(self):
        if self.cloud_logging_and_monitoring:
            return google.cloud.monitoring_v3.MetricServiceClient()
        return None

    @property
    def start_time(self):
        start_time = datetime.max
        for copy_driver in self.__copy_drivers:
            if copy_driver.start_time is not None:
                if copy_driver.start_time < start_time:
                    start_time = copy_driver.start_time
        if start_time == datetime.max:
            return None
        return start_time

    @property
    def end_time(self):
        end_time = datetime.min
        for copy_driver in self.__copy_drivers:
            if copy_driver.end_time is not None:
                if copy_driver.end_time > end_time:
                    end_time = copy_driver.end_time
        if end_time == datetime.min:
            return None
        return end_time

    def reset_stats(self):
        for copy_driver in self.__copy_drivers:
            copy_driver.reset_stats()

    def state(self):
        if self.start_time is None:
            return self.NOTSTARTED
        if self.end_time is None:
            return self.RUNNING
        return self.FINSIHED

    @property
    def sync_time_seconds(self):
        return (self.end_time - self.start_time).seconds

    @property
    def logger(self):
        return self.__copy_drivers[0].logger

    @property
    def check_depth(self):
        return self.__check_depth

    @property
    def query_cache_hits(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.query_cache_hits
        return total

    @property
    def total_bytes_processed(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.total_bytes_processed
        return total

    @property
    def total_bytes_billed(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.total_bytes_billed
        return total

    @check_depth.setter
    def check_depth(self, value):
        self.__check_depth = value
        for copy_driver in self.__copy_drivers:
            copy_driver.check_depth = self.__check_depth

    @logger.setter
    def logger(self, value):
        for copy_driver in self.__copy_drivers:
            copy_driver.logger = value

    @property
    def tables_synced(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.tables_synced
        return total

    @property
    def blob_rewrite_retried_exceptions(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.blob_rewrite_retried_exceptions
        return total

    @property
    def blob_rewrite_unretryable_exceptions(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.blob_rewrite_unretryable_exceptions
        return total

    @property
    def views_synced(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.views_synced
        return total

    @property
    def rows_synced(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.rows_synced
        return total

    @property
    def rows_avoided(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.rows_avoided
        return total

    @property
    def views_failed_sync(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.views_failed_sync
        return total

    @property
    def routines_synced(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.routines_synced
        return total

    @property
    def routines_avoided(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.routines_avoided
        return total

    @property
    def routines_failed_sync(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.routines_failed_sync

        return total

    @property
    def models_synced(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.models_synced
        return total

    @property
    def models_avoided(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.models_avoided
        return total

    @property
    def models_failed_sync(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.models_failed_sync

        return total

    @property
    def tables_failed_sync(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.tables_failed_sync
        return total

    @property
    def tables_avoided(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.tables_avoided
        return total

    @property
    def view_avoided(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.view_avoided
        return total

    @property
    def extract_fails(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.extract_fails
        return total

    @property
    def load_fails(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.load_fails
        return total

    @property
    def copy_fails(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.copy_fails
        return total

    @property
    def bytes_copied_across_region(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.bytes_copied_across_region
        return total

    @property
    def bytes_copied(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.bytes_copied
        return total

    @property
    def files_copied_across_region(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.files_copied_across_region
        return total

    @property
    def bytes_synced(self):
        total = 0
        for copy_driver in self.__copy_drivers:
            total += copy_driver.bytes_synced
        return total

    def sync_monitor_thread(self, stop_event):
        """
        Main background thread driver loop for copying
        :param copy_driver: Basis of copy
        :param q: The work queue tasks are put in
        :return: None
        """

        last_run = datetime.utcnow()
        while not stop_event.isSet():
            try:
                sleep(0.5)
                if self.state != MultiBQSyncCoordinator.NOTSTARTED \
                        and (datetime.utcnow() - last_run).seconds > 60:
                    last_run = datetime.utcnow()
                    self.log_stats()
            except Exception as e:
                self.logger.exception("Exception monitoring MultiBQSync ignoring and continuing")
                pass
        self.logger.info("=============== Sync Completed ================")
        self.log_stats()

    def sync(self):
        """
        Synchronise all the datasets in the driver
        :return:
        """
        self.logger.info("=============== Sync Starting ================")
        # start monitoring thread
        stop_event = threading.Event()
        t = threading.Thread(target=self.sync_monitor_thread, name="monitorthread", args=[
            stop_event])
        t.daemon = True
        t.start()

        self.__copy_drivers[0].logger.info("Calculating differences....")
        for copy_driver in self.__copy_drivers:
            copy_driver.get_logger().info("{}.{} -> {}.{}".format(copy_driver.source_project,
                                                                  copy_driver.source_dataset,
                                                                  copy_driver.destination_project,
                                                                  copy_driver.destination_dataset))
            sync_bq_datset(copy_driver)

        # once copied we can try doing access
        # as all related views hopefully should be done
        for copy_driver in self.__copy_drivers:
            copy_driver.copy_access_to_destination()

        # stop the monitoring thread
        stop_event.set()
        t.join()

    def log_stats(self):

        # provide some stats
        if self.rows_synced - self.rows_avoided == 0:
            speed_up = float('inf')
        else:
            speed_up = float(self.rows_synced) / float(
                self.rows_synced - self.rows_avoided)

        self.logger.info("Tables synced {:,d}".format(self.tables_synced))
        self.logger.info("Tables avoided {:,d}".format(self.tables_avoided))
        self.logger.info("Tables failed {:,d}".format(self.tables_failed_sync))
        self.logger.info("Views synced {:,d}".format(self.views_synced))
        self.logger.info("Views avoided {:,d}".format(self.view_avoided))
        self.logger.info("Views failed {:,d}".format(self.views_failed_sync))
        self.logger.info("Routines synced {:,d}".format(self.routines_synced))
        self.logger.info("Routines avoided {:,d}".format(self.routines_avoided))
        self.logger.info("Routines failed {:,d}".format(self.routines_failed_sync))
        self.logger.info("Models synced {:,d}".format(self.models_synced))
        self.logger.info("Models avoided {:,d}".format(self.models_avoided))
        self.logger.info("Models failed {:,d}".format(self.models_failed_sync))
        self.logger.info(
            "Rows synced {:,d} Total rows as declared in meta data scan phase".format(
                self.rows_synced))
        self.logger.info(
            "Rows Avoided {:,d} Rows avoided in calculation phase".format(
                self.rows_avoided))
        self.logger.info(
            "Rows Difference {:,d}".format(
                self.rows_synced - self.rows_avoided))
        self.logger.info("Bytes Billed {:,d}".format(self.total_bytes_billed))
        self.logger.info(
            "Bytes Processed {:,d}".format(self.total_bytes_processed))
        self.logger.info(
            "Cost of comparison at $5.00 per TB $ {:1,.02f}".format(
                round((self.total_bytes_processed * 5.0) / (1024.0 *
                                                            1024.0 *
                                                            1024.0 *
                                                            1024.0), 2)))
        self.logger.info("Query Cache Hits {:,d}".format(self.query_cache_hits))
        self.logger.info("Extract Fails {:,d}".format(self.extract_fails))
        self.logger.info(
            "Blob retryable exceptions {:,d}".format(self.blob_rewrite_retried_exceptions))
        self.logger.info(
            "Blob unretryable exceptions {:,d}".format(self.blob_rewrite_unretryable_exceptions))
        self.logger.info("Load Fails {:,d}".format(self.load_fails))
        self.logger.info("Copy Fails {:,d}".format(self.copy_fails))
        self.logger.info(
            "Files Copied {:,d}".format(self.files_copied_across_region))
        self.logger.info(
            "Bytes Copied {:1,.02f} GB".format(
                round(self.bytes_copied_across_region / (1024 * 1024 * 1024), 2)))
        self.logger.info(
            "BQ Bytes Synced {:1,.02f} GB".format(
                round(self.bytes_synced / (1024 * 1024 * 1024), 2)))
        self.logger.info(
            "BQ Bytes Uncompressed Copied {:1,.02f} GB".format(
                round(self.bytes_copied / (1024 * 1024 * 1024), 2)))
        self.logger.info("Sync Duration {}".format(
            str(self.end_time - self.start_time)))
        self.logger.info("Rows per Second {:1,.2f}".format(
            round(self.rows_synced / self.sync_time_seconds, 2)))
        self.logger.info("Cross Region (Network) {:1,.3f} Mbs".format(
            round(((self.bytes_copied_across_region * 8.0) / (1024 * 1024)) /
                  self.sync_time_seconds, 3)))
        self.logger.info("Cross Region (Big Query Uncompressed Data) {:1,.3f} Mbs".format(
            round(((self.bytes_copied * 8.0) / (1024 * 1024)) /
                  self.sync_time_seconds, 3)))
        self.logger.info("Speed up {:1,.2f}".format(round(speed_up, 2)))
        if self.cloud_logging_and_monitoring:
            pass

    def create_access_view(self, entity_id):
        access = None

        for copy_driver in self.__copy_drivers:
            if access is not None:
                entity_id = access.entity_id
            access = copy_driver.real_create_access_view(entity_id)

        return access

    def update_source_view_definition(self, view_definition, use_standard_sql):
        # handle projects and dasets in the copy
        for copy_driver in self.__copy_drivers:
            view_definition = copy_driver.real_update_source_view_definition(view_definition,
                                                                             use_standard_sql)

        # handle referenced projects and datsets if any
        for numproj,src_proj_dataset, src_dataset in enumerate(self.__src_ref_project_datasets):
            dst_proj_dataset = self.__dst_ref_project_datasets[numproj]
            src_proj,src_dataset = src_proj_dataset.split(".")
            dst_proj, dst_dataset = dst_proj_dataset.split(".")
            view_definition = view_definition.replace(
                r'`{}.{}.'.format(src_proj, src_dataset),
                "`{}.{}.".format(dst_proj, dst_dataset))
            view_definition = view_definition.replace(
                r'[{}:{}.'.format(src_proj, src_dataset),
                "[{}:{}.".format(dst_proj, dst_dataset))
            # this should not be required but seems it is
            view_definition = view_definition.replace(
                r'[{}.{}.'.format(src_proj, src_dataset),
                "[{}:{}.".format(dst_proj, dst_dataset))
        return view_definition


class MultiBQSyncDriver(DefaultBQSyncDriver):
    def __init__(self, srcproject, srcdataset, dstdataset, dstproject=None,
                 srcbucket=None, dstbucket=None, remove_deleted_tables=True,
                 copy_data=True,
                 copy_types=["TABLE", "VIEW", "ROUTINE", "MODEL"],
                 check_depth=-1,
                 copy_access=True,
                 coordinator=None,
                 table_view_filter=[".*"],
                 table_or_views_to_exclude=[],
                 latest_date=None,
                 days_before_latest_day=None,
                 day_partition_deep_check=False,
                 analysis_project=None):
        DefaultBQSyncDriver.__init__(self, srcproject, srcdataset, dstdataset, dstproject,
                                     srcbucket, dstbucket, remove_deleted_tables,
                                     copy_data,
                                     copy_types=copy_types,
                                     check_depth=check_depth,
                                     copy_access=copy_access,
                                     table_view_filter=table_view_filter,
                                     table_or_views_to_exclude=table_or_views_to_exclude,
                                     latest_date=latest_date,
                                     days_before_latest_day=days_before_latest_day,
                                     day_partition_deep_check=day_partition_deep_check,
                                     analysis_project=analysis_project)
        self.__coordinater = coordinator


    @property
    def coordinater(self):
        return self.__coordinater

    @coordinater.setter
    def coordinater(self, value):
        self.__coordinater = value

    def real_update_source_view_definition(self, view_definition, use_standard_sql):
        view_definition = view_definition.replace(
            r'`{}.{}.'.format(self.source_project, self.source_dataset),
            "`{}.{}.".format(self.destination_project, self.destination_dataset))
        view_definition = view_definition.replace(
            r'[{}:{}.'.format(self.source_project, self.source_dataset),
            "[{}:{}.".format(self.destination_project, self.destination_dataset))
        # this should not be required but seems it is
        view_definition = view_definition.replace(
            r'[{}.{}.'.format(self.source_project, self.source_dataset),
            "[{}:{}.".format(self.destination_project, self.destination_dataset))
        return view_definition

    def real_create_access_view(self, entity_id):
        """
        Convert an old view authorised view
        to a new one i.e. change project id

        :param entity_id:
        :return: a view {
        ...     'projectId': 'my-project',
        ...     'datasetId': 'my_dataset',
        ...     'tableId': 'my_table'
        ... }
        """
        if entity_id["projectId"] == self.source_project and entity_id[
            "datasetId"] == self.source_dataset:
            entity_id["projectId"] = self.destination_project
            entity_id["datasetId"] = self.destination_dataset

        return bigquery.AccessEntry(None, 'view', entity_id)

    def create_access_view(self, entity_id):
        return self.coordinater.create_access_view(entity_id)

    def update_source_view_definition(self, view_definition, use_standard_sql):
        view_defintion = self.coordinater.update_source_view_definition(view_definition, use_standard_sql)
        return view_defintion


def create_and_copy_table(copy_driver, table_name):
    """
    Function to create table in destination dataset and copy all data
    Only called for brand new tables

    :param copy_driver: Has checked source dest all exist
    :param table_name: The table name we are copying from source
    :return: None
    """
    assert isinstance(copy_driver,
                      DefaultBQSyncDriver), "Copy driver has to be a subclass of DefaultCopy " \
                                            "Driver"

    dst_dataset_impl = copy_driver.destination_dataset_impl

    srctable_ref = copy_driver.source_client.dataset(copy_driver.source_dataset).table(table_name)
    srctable = copy_driver.source_client.get_table(srctable_ref)

    if srctable.table_type != 'TABLE':
        if srctable.table_type in copy_driver.copy_types:
            if srctable.table_type == "MODEL":
                create_and_copy_model(copy_driver, table_name)
            else:
                copy_driver.get_logger().warning(
                    "Unable to copy a non table of type {} name {}.{}.{}".format(
                        srctable.table_type, copy_driver.source_project, copy_driver.source_dataset,
                        table_name))
                copy_driver.increment_tables_failed_sync()
    else:
        NEW_SCHEMA = list(srctable.schema)
        export_import_type = copy_driver.export_import_format_supported(srctable)
        destination_table_ref = copy_driver.destination_client.dataset(
            copy_driver.destination_dataset).table(table_name)
        destination_table = bigquery.Table(destination_table_ref, schema=NEW_SCHEMA)
        destination_table.description = srctable.description
        destination_table.friendly_name = srctable.friendly_name
        destination_table.labels = srctable.labels
        destination_table.partitioning_type = srctable.partitioning_type
        if srctable.partition_expiration is not None:
            destination_table.partition_expiration = srctable.partition_expiration
        destination_table.expires = srctable.expires
        # handle older library backward cmpatability
        if getattr(srctable, "require_partition_filter", None):
            destination_table.require_partition_filter = srctable.require_partition_filter
        if getattr(srctable, "range_partitioning", None):
            destination_table.range_partitioning = srctable.range_partitioning
        if getattr(srctable, "time_partitioning", None):
            destination_table.time_partitioning = srctable.time_partitioning
        if getattr(srctable, "clustering_fields", None):
            destination_table.clustering_fields = srctable.clustering_fields
        encryption_config = srctable.encryption_configuration

        # encryption configs can be location specific
        if encryption_config is not None:
            destination_table.encryption_config  = \
                copy_driver.calculate_target_cmek_config(encryption_config)

        # and create the table
        try:
            copy_driver.destination_client.create_table(destination_table)
        except Exception:
            copy_driver.increment_tables_failed_sync()
            raise

        copy_driver.get_logger().info(
            "Created table {}.{}.{}".format(copy_driver.destination_project,
                                            copy_driver.destination_dataset,
                                            table_name))

        # anything to copy we have just created table
        # so destination we know is 0
        if srctable.num_rows != 0:
            copy_driver.add_bytes_synced(srctable.num_bytes)
            copy_driver.add_rows_synced(srctable.num_rows)
            copy_driver.copy_q.put((-1 * srctable.num_rows, BQSyncTask(copy_table_data,
                                                                       [copy_driver, table_name,
                                                                        srctable.partitioning_type,
                                                                        0,
                                                                        srctable.num_rows,
                                                                        export_import_type])))


def create_and_copy_model(copy_driver, model_name):
    if getattr(bigquery, "Model", None):
        copy_driver.get_logger().error(
            "Unable to copy Model {}.{}.{} as meta data of input to model for training is not "
            "provided by the model meta data calls form big query skipping".format(
                copy_driver.source_project, copy_driver.source_dataset,
                model_name))
        copy_driver.increment_models_failed_sync()
    else:
        copy_driver.get_logger().error(
            "Unable to copy Model {}.{}.{} as current python environment big query library does "
            "not support Models".format(copy_driver.source_project, copy_driver.source_dataset,
                                        model_name))
        copy_driver.increment_models_failed_sync()


def compare_model_patch_ifneeded(copy_driver, model_name):
    remove_deleted_destination_table(copy_driver, model_name)
    create_and_copy_model(copy_driver, model_name)


def compare_schema_patch_ifneeded(copy_driver, table_name):
    """
    Compares schemas and patches if needed and copies data
    :param copy_driver:
    :param table_name:
    :return:
    """
    srctable_ref = copy_driver.source_client.dataset(copy_driver.source_dataset).table(table_name)
    srctable = copy_driver.source_client.get_table(srctable_ref)
    dsttable_ref = copy_driver.destination_client.dataset(copy_driver.destination_dataset).table(
        table_name)
    dsttable = copy_driver.source_client.get_table(dsttable_ref)

    # if different table types thats not good need to sort
    # drop and recreate this handles TABLE->MODEL and MODEL->TABLE
    if dsttable.table_type != srctable.table_type:
        copy_driver.get_logger().warning(
            "Change in table_type source {0}.{1}.{tablename} is type {2} and destination {3}.{"
            "4}.{tablename} is type {5}".format(
                copy_driver.source_project,
                copy_driver.source_dataset,
                srctable.table_type,
                copy_driver.destination_project,
                copy_driver.destination_dataset,
                dsttable.table_type,
                table_name=table_name))
        remove_deleted_destination_table(copy_driver, table_name)
        create_and_copy_table(copy_driver, table_name)
        return

    if srctable.table_type == "MODEL":
        compare_model_patch_ifneeded(copy_driver, table_name)
        return

    NEW_SCHEMA = list(srctable.schema)
    OLD_SCHEMA = list(dsttable.schema)

    fields = []
    # Only check encryption if missing if its has been updated but exists left as is
    if srctable.encryption_configuration is not None and dsttable.encryption_configuration is None:
        dsttable.encryption_configuration = copy_driver.calculate_target_cmek_config(srctable.encryption_configuration)
        fields.append("encryption_configuration")
    if dsttable.description != srctable.description:
        dsttable.description = srctable.description
        fields.append("description")
    if dsttable.friendly_name != srctable.friendly_name:
        dsttable.friendly_name = srctable.friendly_name
        fields.append("friendly_name")
    if dsttable.labels != srctable.labels:
        dsttable.labels = srctable.labels
        fields.append("labels")
    if dsttable.partition_expiration != srctable.partition_expiration:
        dsttable.partition_expiration = srctable.partition_expiration
        fields.append("partition_expiration")
    if dsttable.expires != srctable.expires:
        dsttable.expires = srctable.expires
        fields.append("expires")

    # if fields added lengths will differ
    # as initial copy used original these will be same order
    # merge and comare schemas
    def compare_and_merge_schema_fields(input):
        working_schema = []
        changes = 0

        field_names_found = {}

        for schema_item in input["oldchema"]:
            match = False
            for tgt_schema_item in input["newschema"]:
                if tgt_schema_item.name == schema_item.name:
                    field_names_found[schema_item.name] = True
                    match = True
                    # cannot patch type changes so have to recreate
                    if tgt_schema_item.field_type != schema_item.field_type:
                        changes += 1
                        input["workingchema"] = working_schema
                        input["changes"] = changes
                        input["deleteandrecreate"] = True
                        return input
                    if tgt_schema_item.description != schema_item.description:
                        changes += 1
                    if tgt_schema_item.field_type != "RECORD":
                        working_schema.append(tgt_schema_item)
                    else:
                        # cannot change mode for record either repeated or not
                        if tgt_schema_item.mode != schema_item.mode:
                            changes += 1
                            input["workingchema"] = working_schema
                            input["changes"] = changes
                            input["deleteandrecreate"] = True
                            return input

                        output = compare_and_merge_schema_fields(
                            {"oldchema": list(schema_item.fields),
                             "newschema": list(tgt_schema_item.fields)})
                        changes += output["changes"]
                        # if changes then need to create a new schema with new fields
                        # field is immutable so convert to api rep
                        # alter and convert back
                        if output["changes"] > 0:
                            newfields = []
                            for schema_item in output["workingchema"]:
                                newfields.append(schema_item.to_api_repr())
                            tmp_work = tgt_schema_item.to_api_repr()
                            tmp_work["fields"] = newfields
                            working_schema.append(bigquery.SchemaField.from_api_repr(tmp_work))
                        else:
                            working_schema.append(tgt_schema_item)
                        if "deleteandrecreate" in output and output["deleteandrecreate"]:
                            input["deleteandrecreate"] = output["deleteandrecreate"]
                            return input

                    break

        # retain stuff that existed previously
        # nominally a change but as not an addition deemed not to be
        if not match:
            working_schema.append(schema_item)

        # add any new structures
        for tgt_schema_item in input["newschema"]:
            if tgt_schema_item.name not in field_names_found:
                working_schema.append(tgt_schema_item)
                changes += 1

        if len(working_schema) < len(input["newschema"]):
            pass

        input["workingchema"] = working_schema
        input["changes"] = changes
        return input

    output = compare_and_merge_schema_fields({"oldchema": OLD_SCHEMA, "newschema": NEW_SCHEMA})
    if "deleteandrecreate" in output:
        remove_deleted_destination_table(copy_driver, table_name)
        create_and_copy_table(copy_driver, table_name)
        return
    else:
        if output["changes"] > 0:
            dsttable.schema = output["workingchema"]
            fields.append("schema")

        # and update the table
        if len(fields) > 0:
            try:
                copy_driver.destination_client.update_table(dsttable,
                                                            fields)
            except exceptions.BadRequest as e:
                if "encryption_configuration" in fields and \
                    str(e).find("Changing from Default to Cloud KMS encryption key and back must be done via table.copy job") != -1:
                    pass
                else:
                    copy_driver.increment_tables_failed_sync()
                    raise
            except Exception as e:
                copy_driver.increment_tables_failed_sync()
                raise

            dsttable = copy_driver.source_client.get_table(dsttable_ref)
            copy_driver.get_logger().info(
                "Patched table {}.{}.{} {}".format(copy_driver.destination_project,
                                                   copy_driver.destination_dataset,
                                                   table_name, ",".join(fields)))

        copy_driver.add_bytes_synced(srctable.num_bytes)
        copy_driver.add_rows_synced(srctable.num_rows)

        # as not possible to patch on day partition with data time to rebuild this table
        # this should be feasible with an in region copy
        if "encryption_configuration" in fields and \
                dsttable.num_rows != 0 and \
                (dsttable.partitioning_type == "DAY" or
                 dsttable.encryption_configuration is None): # going from none to some needs to
                                                             # happen via a copy
                update_table_cmek_via_copy(copy_driver.destination_client,
                                           dsttable,
                                           copy_driver.calculate_target_cmek_config(
                                                    srctable.encryption_configuration),
                                           copy_driver.get_logger())
                dsttable = copy_driver.source_client.get_table(dsttable_ref)

        if dsttable.num_rows != srctable.num_rows or \
                dsttable.num_bytes != srctable.num_bytes or \
                srctable.modified >= dsttable.modified or \
                copy_driver.table_data_change(srctable, dsttable):
            export_import_type = copy_driver.export_import_format_supported(srctable,dsttable)
            copy_driver.copy_q.put((-1 * srctable.num_rows, BQSyncTask(copy_table_data,
                                                                       [copy_driver, table_name,
                                                                        srctable.partitioning_type,
                                                                        dsttable.num_rows,
                                                                        srctable.num_rows,
                                                                        export_import_type])))
        else:
            copy_driver.increment_tables_avoided()
            copy_driver.add_bytes_avoided(srctable.num_bytes)
            copy_driver.add_rows_avoided(srctable.num_rows)


def update_table_cmek_via_copy(client,
                               srctable,
                               encryption_configuration,
                               logger):
    """
    being asked to update a day partition tables cmek copy it within dataset
    recreate and copy back
    :param table_to_update:
    :param encryption_configuration:
    :return:
    """

    # copy to a temp table
    dsttable = client.dataset(srctable.dataset_id).table("zz" + srctable.table_id + "_temp")
    if table_exists(client, dsttable):
        client.delete_table(dsttable)

    job_config = bigquery.CopyJobConfig()
    job_config.destination_encryption_configuration = encryption_configuration
    job = client.copy_table(
        srctable, dsttable, job_config=job_config,
        location=client.get_dataset(client.dataset(srctable.dataset_id)).location)

    wait_for_jobs([job],logger=logger)

    dsttable_id = srctable.table_id
    srctable = dsttable
    dsttable = client.dataset(srctable.dataset_id).table(dsttable_id)
    if table_exists(client, dsttable):
        client.delete_table(dsttable)
    job_config = bigquery.CopyJobConfig()
    job_config.destination_encryption_configuration = encryption_configuration
    job = client.copy_table(
        srctable, dsttable, job_config=job_config,
        location=client.get_dataset(client.dataset(srctable.dataset_id)).location)

    wait_for_jobs([job],logger=logger)

    client.delete_table(srctable)

def remove_deleted_destination_table(copy_driver, table_name):
    """
    Removes tables/views deleted on source dataset but that exist on destination dataset
    :param copy_driver:
    :param table_name:
    :return:
    """
    if copy_driver.remove_deleted_tables:
        table_ref = copy_driver.destination_dataset_impl.table(table_name)
        table = copy_driver.destination_client.get_table(table_ref)
        copy_driver.destination_client.delete_table(table)
        copy_driver.get_logger().info(
            "Deleted table/view {}.{}.{}".format(copy_driver.destination_project,
                                                 copy_driver.destination_dataset,
                                                 table_name))


# need to be able to compare close numbers
# https://stackoverflow.com/questions/5595425/what-is-the-best-way-to-compare-floats-for-almost
# -equality-in-python
#
def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def copy_table_data(copy_driver,
                    table_name,
                    partitioning_type,
                    dst_rows,
                    src_rows,
                    export_import_format):
    """
    Function copies data assumes schemas are identical
    :param copy_driver:
    :param table_name:
    :return:
    """
    if not copy_driver.copy_data:
        copy_driver.logger.info(
            "Would have copied table {}.{}.{} to {}.{}.{} source rows {} destination rows {}".
                format(copy_driver.source_project,
                       copy_driver.source_dataset,
                       table_name,
                       copy_driver.destination_project,
                       copy_driver.destination_dataset,
                       table_name,
                       src_rows,
                       dst_rows))
        return

    src_query = None
    dst_query = None
    if partitioning_type == "DAY":
        bqclient = copy_driver.source_client
        srctable_ref = bqclient.dataset(copy_driver.source_dataset).table(table_name)
        srctable = bqclient.get_table(srctable_ref)
        partitiontime, extrafields, extrajoinandpredicates = copy_driver.extra_dp_compare_functions(
            srctable)
        src_query = TCMPDAYPARTITION.format(partitiontime=partitiontime,
                                            project=copy_driver.source_project,
                                            dataset=copy_driver.source_dataset,
                                            extrafunctions=extrafields,
                                            extrajoinandpredicates=extrajoinandpredicates,
                                            table_name=table_name)
        dst_query = TCMPDAYPARTITION.format(partitiontime=partitiontime,
                                            project=copy_driver.destination_project,
                                            dataset=copy_driver.destination_dataset,
                                            extrafunctions=extrafields,
                                            extrajoinandpredicates=extrajoinandpredicates,
                                            table_name=table_name)

    # if same region now just do a table copy
    if copy_driver.same_region:
        jobs = []
        if partitioning_type != "DAY" or (
                partitioning_type == "DAY" and dst_rows == 0 and
                len(copy_driver.comparison_predicates(table_name)) == 0) or (
                partitioning_type == "DAY" and src_rows <= 5000 and
                len(copy_driver.comparison_predicates(table_name)) == 0):
            jobs.append(in_region_copy(copy_driver, table_name))
        else:
            source_ended = False
            destination_ended = False

            source_generator = run_query(copy_driver.query_client, src_query,
                                         copy_driver.get_logger(),
                                         "List source data per partition",
                                         location=copy_driver.source_location,
                                         callback_on_complete=copy_driver.update_job_stats,
                                         labels=BQSYNCQUERYLABELS)
            try:
                source_row = next(source_generator)
            except StopIteration:
                source_ended = True

            destination_generator = run_query(copy_driver.query_client, dst_query,
                                              copy_driver.get_logger(),
                                              "List destination data per partition",
                                              location=copy_driver.destination_location,
                                              callback_on_complete=copy_driver.update_job_stats,
                                              labels=BQSYNCQUERYLABELS)
            try:
                destination_row = next(destination_generator)
            except StopIteration:
                destination_ended = True

            while not (source_ended and destination_ended):
                if not destination_ended and not source_ended and destination_row[
                    "partitionName"] == \
                        source_row["partitionName"]:
                    diff = False
                    rowdict = dict(list(source_row.items()))
                    for key in rowdict:
                        # if different
                        # if not a float thats ok
                        # but if a float assume math so has rounding truncation
                        # type errors so only check within float
                        # accuracy tolerance
                        if source_row[key] != destination_row[key] and (
                                not isinstance(source_row[key], float) or not isclose(
                            source_row[key],
                            destination_row[
                                key])):
                            diff = True
                            copy_driver.logger.info(
                                "Copying {}.{}.{}${} to {}.{}.{}${} difference in {} source value "
                                "{} "
                                "destination value {}".format(
                                    copy_driver.source_project,
                                    copy_driver.source_dataset,
                                    table_name,
                                    source_row[
                                        "partitionName"],
                                    copy_driver.destination_project,
                                    copy_driver.destination_dataset,
                                    table_name,
                                    source_row[
                                        "partitionName"],
                                    key,
                                    source_row[key],
                                    destination_row[key]))
                            break
                    if diff:
                        jobs.append(in_region_copy(copy_driver,
                                                   "{}${}".format(table_name,
                                                                  source_row["partitionName"])))
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True
                elif (destination_ended and not source_ended) or (
                        not source_ended and source_row["partitionName"] < destination_row[
                    "partitionName"]):
                    copy_driver.logger.info(
                        "Copying {}.{}.{}${} to {}.{}.{}${} as not on destination".format(
                            copy_driver.source_project,
                            copy_driver.source_dataset,
                            table_name,
                            source_row[
                                "partitionName"],
                            copy_driver.destination_project,
                            copy_driver.destination_dataset,
                            table_name,
                            source_row[
                                "partitionName"]))
                    jobs.append(in_region_copy(copy_driver,
                                               "{}${}".format(table_name,
                                                              source_row["partitionName"])))
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                elif (source_ended and not destination_ended) or (
                        not destination_ended and source_row["partitionName"] > destination_row[
                    "partitionName"]):
                    remove_deleted_destination_table(
                        copy_driver, "{}${}".format(table_name, destination_row["partitionName"]))
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True

        def copy_complete_callback(job):
            copy_driver.update_job_stats(job)

        callbacks = {}
        for job in jobs:
            callbacks[job.job_id] = copy_complete_callback

        wait_for_jobs(jobs, copy_driver.logger, desc="Wait for copy jobs",
                      call_back_on_complete=callbacks)

    # else not same region so have to extract
    # copy object and
    # load
    else:
        if partitioning_type != "DAY":
            cross_region_copy(copy_driver, table_name,export_import_format)
        else:
            source_ended = False
            destination_ended = False

            source_generator = run_query(copy_driver.query_client, src_query,
                                         copy_driver.get_logger(),
                                         "List source data per partition",
                                         location=copy_driver.source_location,
                                         callback_on_complete=copy_driver.update_job_stats,
                                         labels=BQSYNCQUERYLABELS)
            try:
                source_row = next(source_generator)
            except StopIteration:
                source_ended = True

            destination_generator = run_query(copy_driver.query_client, dst_query,
                                              copy_driver.get_logger(),
                                              "List destination data per partition",
                                              location=copy_driver.destination_location,
                                              callback_on_complete=copy_driver.update_job_stats,
                                              labels=BQSYNCQUERYLABELS)
            try:
                destination_row = next(destination_generator)
            except StopIteration:
                destination_ended = True

            while not (source_ended and destination_ended):
                if not destination_ended and not source_ended and destination_row[
                    "partitionName"] == \
                        source_row["partitionName"]:
                    diff = False
                    rowdict = dict(list(source_row.items()))
                    for key in sorted(rowdict.keys()):
                        # if different
                        # if not a float thats ok
                        # but if a float assume math so has rounding truncation
                        # type errors so only check within float
                        # accuracy tolerance
                        if source_row[key] != destination_row[key] and (
                                not isinstance(source_row[key], float) or not isclose(
                            source_row[key],
                            destination_row[
                                key])):
                            diff = True
                            copy_driver.logger.info(
                                "Copying {}.{}.{}${} to {}.{}.{}${} difference in {} source value "
                                "{} "
                                "destination value {}".format(
                                    copy_driver.source_project,
                                    copy_driver.source_dataset,
                                    table_name,
                                    source_row[
                                        "partitionName"],
                                    copy_driver.destination_project,
                                    copy_driver.destination_dataset,
                                    table_name,
                                    source_row[
                                        "partitionName"],
                                    key,
                                    source_row[key],
                                    destination_row[key]))
                            break
                    if diff:
                        copy_driver.copy_q.put(
                            (-1 * source_row["rowNum"], BQSyncTask(cross_region_copy,
                                                                   [copy_driver,
                                                                    "{}${}".format(
                                                                        table_name,
                                                                        source_row[
                                                                            "partitionName"]),
                                                                    export_import_format])))
                    else:
                        copy_driver.add_rows_avoided(source_row["rowNum"])
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True
                elif (destination_ended and not source_ended) or (
                        not source_ended and source_row["partitionName"] < destination_row[
                    "partitionName"]):
                    copy_driver.logger.info(
                        "Copying {}.{}.{}${} to {}.{}.{}${} as not on destination".format(
                            copy_driver.source_project,
                            copy_driver.source_dataset,
                            table_name,
                            source_row[
                                "partitionName"],
                            copy_driver.destination_project,
                            copy_driver.destination_dataset,
                            table_name,
                            source_row[
                                "partitionName"]))
                    copy_driver.copy_q.put((-1 * source_row["rowNum"], BQSyncTask(cross_region_copy,
                                                                                  [copy_driver,
                                                                                   "{}${}".format(
                                                                                       table_name,
                                                                                       source_row[
                                                                                           "partitionName"],
                                                                                   ),
                                                                                   export_import_format])))
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                elif (source_ended and not destination_ended) or (
                        not destination_ended and source_row["partitionName"] > destination_row[
                    "partitionName"]):
                    remove_deleted_destination_table(
                        copy_driver, "{}${}".format(table_name, destination_row["partitionName"]))
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True


def cross_region_copy(copy_driver, table_name, export_import_type):
    """
    Copy data acoss region each table is
    Extracted out to avro file to cloud storage src
    Data is loaded into new dataset
    Deleted from destination
    :param copy_driver:
    :param table_name:
    :return:
    """

    bqclient = copy_driver.source_client
    blobname = "{}-{}-{}-{}-{}*.avro".format(table_name, copy_driver.source_project,
                                             copy_driver.source_dataset,
                                             copy_driver.destination_project,
                                             copy_driver.destination_dataset)
    src_uri = "gs://{}/{}".format(copy_driver.source_bucket, blobname)
    srctable = bqclient.dataset(copy_driver.source_dataset).table(table_name)
    job_config = bigquery.ExtractJobConfig()
    # using logical types expads clumn types supported
    # but has no DATETIME support
    # do logic that is avros speific
    if export_import_type.destination_format == bigquery.job.DestinationFormat.AVRO:
        job_config.use_avro_logical_types = True

    # use avros as parallel read/write
    # worth noting avro does not support DATETIME
    job_config.destination_format = export_import_type.destination_format
    # compress trade compute for network bandwidth
    job_config.compression = export_import_type.compression_format

    # start the extract
    job = copy_driver.query_client.extract_table(srctable, [src_uri], job_config=job_config,
                                                 location=copy_driver.source_location)

    def cross_region_rewrite(job):
        if job.error_result:
            copy_driver.increment_extract_fails()
        else:
            blob_uris = int(job._job_statistics().get('destinationUriFileCounts')[0])

            def generate_cp_files():
                """Generate sources and destinations."""
                for blob_num in range(blob_uris):
                    ablobname = blobname.replace("*", "{:012d}".format(blob_num))
                    yield ablobname

            def rewrite_blob(new_name):
                total_bytes = 0
                bytes_rewritten = 0
                retry = True
                loop = 0

                while (retry):
                    retry = False
                    try:
                        client = storage.Client(project=copy_driver.source_project)

                        src_bucket = client.get_bucket(copy_driver.source_bucket)
                        dst_bucket = client.get_bucket(copy_driver.destination_bucket)
                        src_blob = storage.blob.Blob(new_name, src_bucket)

                        dst_blob = storage.blob.Blob(new_name, dst_bucket)

                        (token, bytes_rewritten, total_bytes) = dst_blob.rewrite(src_blob)

                        # wait for rewrite to finish
                        while token is not None:
                            (token, bytes_rewritten, total_bytes) = dst_blob.rewrite(src_blob,
                                                                                     token=token)

                    # retry based upon
                    # https://cloud.google.com/storage/docs/json_api/v1/status-codes
                    except (exceptions.BadGateway,
                            exceptions.GatewayTimeout,
                            exceptions.InternalServerError,
                            exceptions.TooManyRequests,
                            exceptions.ServiceUnavailable,
                            requests.exceptions.SSLError) as e:
                        # exponential back off for these
                        copy_driver.increment_blob_rewrite_retried_exceptions()
                        loop = loop + 1
                        sleep_time_secs = min(random.random() * (2 ** loop), 32.0)
                        copy_driver.get_logger().exception(
                            "Retryable exception  re-writing blob in cross region copy gs://{}/{} "
                            "to gs://{}/{"
                            "} backing off {}".format(
                                copy_driver.source_bucket,
                                new_name,
                                copy_driver.destination_bucket,
                                new_name,
                                sleep_time_secs))
                        sleep(sleep_time_secs)
                        retry = True
                    except Exception as e:
                        copy_driver.increment_blob_rewrite_unretryable_exceptions()
                        copy_driver.get_logger().exception(
                            "Exception re-writing blob in cross region copy gs://{}/{} to gs://{}/{"
                            "}".format(
                                copy_driver.source_bucket,
                                new_name, copy_driver.destination_bucket, new_name))

                    # handle fact not even got a blob object this implies something bad has
                    # happend but logging will have repoted this anyhow
                    try:
                        src_blob.delete()
                    except UnboundLocalError:
                        pass

            # set worker for small 1 is good enough as volume grows cap the max
            # grows dynamically up to 100 then caps at 20
            max_workers = min(max(int(blob_uris / 5), 1), 20)

            # None is ThreadPoolExecutor max_workers default. 1 is single-threaded.
            # this fires up a number of background threads to rewrite all the blobs
            # we keep storage client work withi
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
                    as executor:
                futures = [
                    executor.submit(
                        rewrite_blob,
                        new_name=blob_name)
                    for blob_name in generate_cp_files()
                ]

                for future in futures:
                    _ = future.result()

            dst_uri = "gs://{}/{}".format(copy_driver.destination_bucket, blobname)
            bqclient = copy_driver.destination_client
            dsttable = bqclient.dataset(copy_driver.destination_dataset).table(table_name)
            job_config = bigquery.LoadJobConfig()

            if export_import_type.source_format == bigquery.job.SourceFormat.AVRO:
                job_config.use_avro_logical_types = True
            job_config.source_format = export_import_type.source_format
            # compress trade compute for network bandwidth
            job_config.compression = export_import_type.compression_format
            job_config.schema = export_import_type.schema

            # this is required but nee dto sort patching of cmek first
            if export_import_type.encryption_configuration is not None:
                job_config.destination_encryption_configuration = copy_driver. \
                    calculate_target_cmek_config(export_import_type.encryption_configuration)


            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER
            if getattr(job_config, "schema_update_options", None):
                job_config.schema_update_options = bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                job_config.schema_update_options = \
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION

            job = copy_driver.query_client.load_table_from_uri([dst_uri], dsttable,
                                                               job_config=job_config,
                                                               location=copy_driver.destination_location)

        def delete_blob(job):
            copy_driver.update_job_stats(job)

            if not job.error_result:
                client = storage.Client(project=copy_driver.source_project)
                dst_bucket = client.get_bucket(copy_driver.destination_bucket)
                for blob_num in range(blob_uris):
                    ablobname = blobname.replace("*", "{:012d}".format(blob_num))
                    dst_blob = storage.blob.Blob(ablobname, dst_bucket)
                    dst_blob.delete()

        callbackobj = {job.job_id: delete_blob}
        wait_for_jobs([job], copy_driver.get_logger(),
                      desc="Wait for load job for table {}.{}.{}".format(
                          copy_driver.destination_project,
                          copy_driver.destination_dataset,
                          table_name),
                      call_back_on_complete=callbackobj)

    callbackobj = {job.job_id: cross_region_rewrite}
    wait_for_jobs([job], copy_driver.get_logger(),
                  desc="Wait for extract job for table {}.{}.{}".format(
                      copy_driver.source_project,
                      copy_driver.source_dataset,
                      table_name),
                  call_back_on_complete=callbackobj)

    return


def in_region_copy(copy_driver, table_name):
    srctable = copy_driver.source_client.dataset(copy_driver.source_dataset).table(table_name)
    dsttable = copy_driver.destination_client.dataset(copy_driver.destination_dataset).table(
        table_name)
    # Move to use write disposition so can haved ifferent write modes
    # this allows day partition snapshot and append tables
    # if self.table_exists(client, dsttable):
    #    client.delete_table(dsttable)
    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.create_disposition = 'CREATE_IF_NEEDED'

    job = copy_driver.query_client.copy_table(
        srctable, dsttable, job_config=job_config)

    return job


def sync_bq_processor(stop_event, copy_driver, q):
    """
    Main background thread driver loop for copying
    :param copy_driver: Basis of copy
    :param q: The work queue tasks are put in
    :return: None
    """
    assert isinstance(copy_driver,
                      DefaultBQSyncDriver), "Copy driver has to be a subclass of DefaultCopy " \
                                            "Driver"
    assert isinstance(q,
                      queue.Queue), "Must be passed a queue for background thread " \
                                    "Driver"

    while not stop_event.isSet():
        try:
            try:
                _, task = q.get(timeout=1)
                assert isinstance(task, BQSyncTask)
            except TypeError as e:
                if str(e).find(
                        "not supported between instances of 'function' and 'function'") == -1:
                    raise
                else:
                    copy_driver.logger.exception(
                        "Swallowing type exception cause by bug in Priority Queue")

            if task is None:
                continue

            function = task.function
            args = task.args
            copy_driver.fault_barrier(function, *args)
            q.task_done()

        except queue.Empty:
            pass


def wait_for_jobs(jobs, logger, desc="", sleepTime=0.1, call_back_on_complete=None):
    """
    Wait for a list of big query jobs to complete
    :param jobs: The array of jobs to wait for
    :param desc: A descriptive text of jobs waiting for
    :param sleepTime: How long to sleep between checks of jobs
    :param logger: A logger to use for output
    :return:
    """
    while len(jobs) > 0:
        didnothing = True
        for job in jobs:
            if job is not None:
                if job.done():
                    didnothing = False
                    if job.error_result:
                        logger.error(
                            u"{}:Error BQ {} Job {} error {}".format(desc, job.job_type, job.job_id,
                                                                     str(job.error_result)))
                    else:
                        if (job.job_type == "load") and \
                                job.output_rows is not None:
                            logger.info(
                                u"{}:BQ {} Job completed:{} rows {}ed {:,d}".format(desc,
                                                                                    job.job_type,
                                                                                    job.job_id,
                                                                                    job.job_type,
                                                                                    job.output_rows))
                    if call_back_on_complete is not None and job.job_id in \
                            call_back_on_complete:
                        call_back_on_complete[job.job_id](job)

            else:
                didnothing = False
                logger.debug(u"{}:BQ {} Job completed:{}".format(desc, job.job_type, job.job_id))

        if didnothing:
            sleep(sleepTime)
        else:
            jobs = [x for x in jobs if x.job_id
                    is not None and not x.state == 'DONE']


def wait_for_queue(q, desc=None, sleepTime=0.1, logger=None):
    """
    Wit for background queue to finish
    :param q: The q to wiat for
    :return:
    """
    while q.qsize() > 0:
        qsize = q.qsize()
        if qsize > 0:
            if logger is None and desc is None:
                logger.info("Waiting for {} tasks {} to start".format(qsize, desc))
            sleep(sleepTime)

    if logger is None and desc is None:
        logger.info("Waiting for tasks {} to complete".format(qsize, desc))
    q.join()
    if logger is None and desc is None:
        logger.info("All tasks {} now complete".format(qsize, desc))


def remove_deleted_destination_routine(copy_driver, routine_name):
    # handle old libraries with no routine support
    if getattr(bigquery, "Routine", None):
        dstroutine_ref = bigquery.Routine(
            "{}.{}.{}".format(copy_driver.destination_project, copy_driver.destination_dataset,
                              routine_name))
        copy_driver.destination_client.delete_routine(dstroutine_ref)
    else:
        copy_driver.get_logger().warning(
            "Unable to remove routine from source {}.{}.{} as Routine class not defined in "
            "bigquery library in the current python environment".format(
                copy_driver.destination_project,
                copy_driver.destination_dataset,
                routine_name))
        copy_driver.increment_routines_failed_sync()


def patch_destination_routine(copy_driver, routine_name, routine_input):
    remove_deleted_destination_routine(copy_driver, routine_name)
    create_destination_routine(copy_driver, routine_name, routine_input)


def create_destination_routine(copy_driver, routine_name, routine_input):
    """
    Create a routine based on source routine
    :param copy_driver:
    :param routine:
    :param routine_input:
    :return:
    """
    # handle old libraries with no routine support
    if getattr(bigquery, "Routine", None):
        srcroutine_ref = bigquery.Routine(
            "{}.{}.{}".format(copy_driver.source_project, copy_driver.source_dataset, routine_name))
        srcroutine = copy_driver.source_client.get_routine(srcroutine_ref)
        dstroutine_ref = bigquery.Routine(
            "{}.{}.{}".format(copy_driver.destination_project, copy_driver.destination_dataset,
                              routine_name))
        # dstroutine = bigquery.Routine(dstroutine_ref)
        dstroutine_ref.description = srcroutine.description
        dstroutine_ref.body = routine_input["routine_definition"]
        dstroutine_ref.return_type = srcroutine.return_type
        dstroutine_ref.arguments = srcroutine.arguments
        dstroutine_ref.imported_libraries = srcroutine.imported_libraries
        dstroutine_ref.language = srcroutine.language
        dstroutine_ref.type_ = srcroutine.type_
        dstroutine = copy_driver.destination_client.create_routine(dstroutine_ref)
        return dstroutine
    else:
        copy_driver.get_logger().warning(
            "Unable to copy Routine {}.{}.{} as current python environment big query library does "
            "not support Routines".format(copy_driver.source_project, copy_driver.source_dataset,
                                          routine_name))
        copy_driver.increment_routines_failed_sync()


def create_destination_view(copy_driver, table_name, view_input):
    """
    Create a view assuming it does not exist
    If error happens on creation logs but swallows error on
    basis order application is not perfect
    :param copy_driver:
    :param table_name: name of the view
    :param view_input: A dictionary of form {
        "use_standard_sql":bool,
        "view_definition":sql}
    :return:
    """

    srctable_ref = copy_driver.source_client.dataset(copy_driver.source_dataset).table(table_name)
    srctable = copy_driver.source_client.get_table(srctable_ref)

    use_legacy_sql = True
    if view_input["use_standard_sql"] == "YES":
        use_legacy_sql = False

    destination_table_ref = copy_driver.destination_client.dataset(
        copy_driver.destination_dataset).table(table_name)
    destination_table = bigquery.Table(destination_table_ref)
    destination_table.description = srctable.description
    destination_table.friendly_name = srctable.friendly_name
    destination_table.labels = srctable.labels
    destination_table.view_use_legacy_sql = use_legacy_sql
    destination_table.view_query = view_input["view_definition"]
    # and create the table
    try:
        copy_driver.destination_client.create_table(destination_table)
        copy_driver.get_logger().info(
            "Created view {}.{}.{}".format(copy_driver.destination_project,
                                           copy_driver.destination_dataset, table_name))
    except exceptions.PreconditionFailed as e:
        copy_driver.increment_views_failed_sync()
        copy_driver.get_logger().exception(
            "Pre conditionfailed creating view {}:{}".format(table_name,
                                                             view_input["view_definition"]))
    except exceptions.BadRequest as e:
        copy_driver.increment_views_failed_sync()
        copy_driver.get_logger().exception(
            "Bad request creating view {}:{}".format(table_name, view_input["view_definition"]))
    except exceptions.NotFound as e:
        copy_driver.increment_views_failed_sync()
        copy_driver.get_logger().exception(
            "Not Found creating view {}:{}".format(table_name, view_input["view_definition"]))


def patch_destination_view(copy_driver, table_name, view_input):
    """
    Patch a view assuming it does  exist
    If error happens on creation logs but swallows error on
    basis order application is not perfect
    :param copy_driver:
    :param table_name: name of the view
    :param view_input: A dictionary of form {
        "use_standard_sql":bool,
        "view_definition":sql}
    :return:
    """
    srctable_ref = copy_driver.source_client.dataset(copy_driver.source_dataset).table(table_name)
    srctable = copy_driver.source_client.get_table(srctable_ref)
    dsttable_ref = copy_driver.destination_client.dataset(copy_driver.destination_dataset).table(
        table_name)
    dsttable = copy_driver.source_client.get_table(dsttable_ref)

    use_legacy_sql = True
    if view_input["use_standard_sql"] == "YES":
        use_legacy_sql = False

    fields = []
    if dsttable.description != srctable.description:
        dsttable.description = srctable.description
        fields.append("description")
    if dsttable.friendly_name != srctable.friendly_name:
        dsttable.friendly_name = srctable.friendly_name
        fields.append("friendly_name")
    if dsttable.labels != srctable.labels:
        dsttable.labels = srctable.labels
        fields.append("labels")
    if dsttable.view_use_legacy_sql != use_legacy_sql:
        dsttable.view_use_legacy_sql = use_legacy_sql
        fields.append("use_legacy_sql")
    if dsttable.view_query != view_input["view_definition"]:
        dsttable.view_query = view_input["view_definition"]
        fields.append("view_query")
    # and patch the view
    try:
        if len(fields) > 0:
            copy_driver.destination_client.update_table(dsttable, fields)
            copy_driver.get_logger().info(
                "Patched view {}.{}.{}".format(copy_driver.destination_project,
                                               copy_driver.destination_dataset, table_name))
        else:
            copy_driver.increment_view_avoided()
    except exceptions.PreconditionFailed as e:
        copy_driver.increment_views_failed_sync()
        copy_driver.get_logger().exception(
            "Pre conditionfailed patching view {}.{}.{}".format(copy_driver.destination_project,
                                                                copy_driver.destination_dataset,
                                                                table_name))
    except exceptions.BadRequest as e:
        copy_driver.increment_views_failed_sync()
        copy_driver.get_logger().exception(
            "Bad request patching view {}.{}.{}".format(copy_driver.destination_project,
                                                        copy_driver.destination_dataset,
                                                        table_name))
    except exceptions.NotFound as e:
        copy_driver.increment_views_failed_sync()
        copy_driver.get_logger().exception(
            "Not Found patching view {}.{}.{}".format(copy_driver.destination_project,
                                                      copy_driver.destination_dataset, table_name))


def sync_bq_datset(copy_driver, schema_threads=10, copy_data_threads=50):
    """
    Function to use copy driver  to copy tables from 1 dataset to another
    :param copy_driver:
    :return: Nothing
    """
    assert isinstance(copy_driver,
                      DefaultBQSyncDriver), "Copy driver has to be a subclass of DefaultCopy " \
                                            "Driver"
    copy_driver.start_sync()

    # start by copying tables
    # start by copying structure and once aligned copy data
    # start by creating tables that do not exist
    # done with tape sort comparing table names
    schema_q = queue.PriorityQueue()
    copy_driver.schema_q = schema_q

    thread_list = []
    stop_event = threading.Event()
    for i in range(schema_threads):
        t = threading.Thread(target=sync_bq_processor, name="copyschemabackgroundthread", args=[
            stop_event, copy_driver, schema_q])
        t.daemon = True
        t.start()
        thread_list.append(t)

    copy_q = queue.PriorityQueue()
    copy_driver.copy_q = copy_q

    for i in range(copy_data_threads):
        t = threading.Thread(target=sync_bq_processor, name="copydatabackgroundthread", args=[
            stop_event, copy_driver, copy_q])
        t.daemon = True
        t.start()
        thread_list.append(t)

    source_query = DSTABLELISTQUERY.format(copy_driver.source_project, copy_driver.source_dataset)
    destination_query = DSTABLELISTQUERY.format(copy_driver.destination_project,
                                                copy_driver.destination_dataset)
    source_ended = False
    destination_ended = False

    source_generator = run_query(copy_driver.query_client, source_query, "List source tables",
                                 copy_driver.get_logger(),
                                 location=copy_driver.source_location,
                                 callback_on_complete=copy_driver.update_job_stats,
                                 labels=BQSYNCQUERYLABELS)
    try:
        source_row = next(source_generator)
    except StopIteration:
        source_ended = True

    destination_generator = run_query(copy_driver.query_client, destination_query,
                                      copy_driver.get_logger(),
                                      "List destination tables",
                                      location=copy_driver.destination_location,
                                      callback_on_complete=copy_driver.update_job_stats,
                                      labels=BQSYNCQUERYLABELS)
    try:
        destination_row = next(destination_generator)
    except StopIteration:
        destination_ended = True

    while not (source_ended and destination_ended):
        if not destination_ended and not source_ended and destination_row["table_name"] == \
                source_row["table_name"]:
            if copy_driver.istableincluded(source_row["table_name"]):
                copy_driver.increment_tables_synced()
                schema_q.put(
                    (0, BQSyncTask(compare_schema_patch_ifneeded,
                                   [copy_driver, source_row["table_name"]])))
            try:
                source_row = next(source_generator)
            except StopIteration:
                source_ended = True
            try:
                destination_row = next(destination_generator)
            except StopIteration:
                destination_ended = True

        elif (destination_ended and not source_ended) or (
                not source_ended and source_row["table_name"] < destination_row["table_name"]):
            if copy_driver.istableincluded(source_row["table_name"]):
                copy_driver.increment_tables_synced()
                schema_q.put(
                    (0, BQSyncTask(create_and_copy_table, [copy_driver, source_row["table_name"]])))
            try:
                source_row = next(source_generator)
            except StopIteration:
                source_ended = True

        elif (source_ended and not destination_ended) or (
                not destination_ended and source_row["table_name"] > destination_row[
            "table_name"]):
            copy_driver.increment_tables_synced()
            remove_deleted_destination_table(copy_driver, destination_row["table_name"])
            try:
                destination_row = next(destination_generator)
            except StopIteration:
                destination_ended = True

    wait_for_queue(schema_q, "Table schemas sychronization", 0.1, copy_driver.get_logger())

    if "VIEW" in copy_driver.copy_types or "ROUTINE" in copy_driver.copy_types:

        # Now do views
        # views need applying in order
        # we assume order created is the order
        view_or_routine_order = []

        views_to_apply = {}
        routines_to_apply = {}

        view_order_query = DSVIEWORDER.format(copy_driver.source_project,
                                              copy_driver.source_dataset)

        for viewrow in run_query(copy_driver.query_client, view_order_query,
                                 copy_driver.get_logger(),
                                 "List views in apply order",
                                 location=copy_driver.source_location,
                                 callback_on_complete=copy_driver.update_job_stats,
                                 labels=BQSYNCQUERYLABELS):
            view_or_routine_order.append(viewrow["table_name"])

        if "VIEW" in copy_driver.copy_types:
            # now list and compare views
            source_view_query = DSVIEWLISTQUERY.format(copy_driver.source_project,
                                                       copy_driver.source_dataset)
            destination_view_query = DSVIEWLISTQUERY.format(copy_driver.destination_project,
                                                            copy_driver.destination_dataset)
            source_ended = False
            destination_ended = False

            source_generator = run_query(copy_driver.query_client, source_view_query,
                                         "List source views", copy_driver.get_logger(),
                                         location=copy_driver.source_location,
                                         callback_on_complete=copy_driver.update_job_stats,
                                         labels=BQSYNCQUERYLABELS)
            try:
                source_row = next(source_generator)
            except StopIteration:
                source_ended = True

            destination_generator = run_query(copy_driver.query_client, destination_view_query,
                                              copy_driver.get_logger(),
                                              "List destination views",
                                              location=copy_driver.destination_location,
                                              callback_on_complete=copy_driver.update_job_stats,
                                              labels=BQSYNCQUERYLABELS)
            try:
                destination_row = next(destination_generator)
            except StopIteration:
                destination_ended = True

            while not source_ended or not destination_ended:
                if not destination_ended and not source_ended and destination_row["table_name"] == \
                        source_row["table_name"]:
                    if copy_driver.istableincluded(source_row["table_name"]):
                        copy_driver.increment_views_synced()
                        expected_definition = copy_driver.update_source_view_definition(
                            source_row["view_definition"], source_row["use_standard_sql"])
                        if expected_definition != destination_row["view_definition"]:
                            views_to_apply[source_row["table_name"]] = {
                                "use_standard_sql": source_row["use_standard_sql"],
                                "view_definition": expected_definition, "action": "patch_view"}
                        else:
                            copy_driver.increment_view_avoided()
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True
                elif (destination_ended and not source_ended) or (
                        not source_ended and source_row["table_name"] < destination_row[
                    "table_name"]):
                    if copy_driver.istableincluded(source_row["table_name"]):
                        copy_driver.increment_views_synced()
                        expected_definition = copy_driver.update_source_view_definition(
                            source_row["view_definition"], source_row["use_standard_sql"])
                        views_to_apply[source_row["table_name"]] = {
                            "use_standard_sql": source_row["use_standard_sql"],
                            "view_definition": expected_definition, "action": "create_view"}
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                elif (source_ended and not destination_ended) or (
                        not destination_ended and source_row["table_name"] > destination_row[
                    "table_name"]):
                    remove_deleted_destination_table(copy_driver, destination_row["table_name"])
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True

        if "ROUTINE" in copy_driver.copy_types:
            # now list and compare views
            source_routine_query = RTNCOMPARE.format(copy_driver.source_project,
                                                     copy_driver.source_dataset)
            destination_routine_query = RTNCOMPARE.format(copy_driver.destination_project,
                                                          copy_driver.destination_dataset)
            source_ended = False
            destination_ended = False

            source_generator = run_query(copy_driver.query_client, source_routine_query,
                                         "List source views", copy_driver.get_logger(),
                                         location=copy_driver.source_location,
                                         callback_on_complete=copy_driver.update_job_stats,
                                         labels=BQSYNCQUERYLABELS)
            try:
                source_row = next(source_generator)
            except StopIteration:
                source_ended = True

            destination_generator = run_query(copy_driver.query_client, destination_routine_query,
                                              copy_driver.get_logger(),
                                              "List destination views",
                                              location=copy_driver.destination_location,
                                              callback_on_complete=copy_driver.update_job_stats,
                                              labels=BQSYNCQUERYLABELS)
            try:
                destination_row = next(destination_generator)
            except StopIteration:
                destination_ended = True

            while not source_ended or not destination_ended:
                if not destination_ended and not source_ended and destination_row["routine_name"]\
                 == \
                        source_row["routine_name"]:
                    if copy_driver.istableincluded(source_row["routine_name"]):
                        copy_driver.increment_routines_synced()
                        expected_definition = copy_driver.update_source_view_definition(
                            source_row["routine_definition"], source_row["routine_type"])
                        if expected_definition != destination_row["routine_definition"]:
                            routines_to_apply[source_row["routine_name"]] = {
                                "routine_definition": expected_definition,
                                "routine_type": source_row["routine_type"],
                                "action": "patch_routine"}
                        else:
                            copy_driver.increment_routines_avoided()
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True
                elif (destination_ended and not source_ended) or (
                        not source_ended and source_row["routine_name"] < destination_row[
                    "routine_name"]):
                    if copy_driver.istableincluded(source_row["routine_name"]):
                        copy_driver.increment_routines_synced()
                        expected_definition = copy_driver.update_source_view_definition(
                            source_row["routine_definition"], source_row["routine_type"])
                        routines_to_apply[source_row["routine_name"]] = {
                            "routine_definition": expected_definition,
                            "routine_type": source_row["routine_type"], "action": "create_routine"}
                    try:
                        source_row = next(source_generator)
                    except StopIteration:
                        source_ended = True
                elif (source_ended and not destination_ended) or (
                        not destination_ended and source_row["routine_name"] > destination_row[
                    "routine_name"]):
                    remove_deleted_destination_routine(copy_driver, destination_row["routine_name"])
                    try:
                        destination_row = next(destination_generator)
                    except StopIteration:
                        destination_ended = True

        for view in view_or_routine_order:
            if view in routines_to_apply:
                if routines_to_apply[view]["action"] == "create_routine":
                    create_destination_routine(copy_driver, view, routines_to_apply[view])
                else:
                    patch_destination_routine(copy_driver, view, routines_to_apply[view])
            if view in views_to_apply:
                if views_to_apply[view]["action"] == "create_view":
                    create_destination_view(copy_driver, view, views_to_apply[view])
                else:
                    patch_destination_view(copy_driver, view, views_to_apply[view])

        wait_for_queue(schema_q, "View/Routine schema synchronization", 0.1,
                       copy_driver.get_logger())

    wait_for_queue(copy_q, "Table copying", 0.1, copy_driver.get_logger())

    # stop all the background threads
    stop_event.set()

    # wait for threads to stop
    for t in thread_list:
        t.join()

    copy_driver.schema_q = None
    copy_driver.copy_q = None

    if len(copy_driver.jobs) > 0:
        wait_for_jobs(copy_driver.jobs,
                      copy_driver.get_logger(),
                      desc="Table copying",
                      sleepTime=0.1)

    copy_driver.end_sync()

    return
