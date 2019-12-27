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

import copy
import json
import os
import pprint
import re
from datetime import datetime, date, timedelta, time

from google.cloud import bigquery, exceptions
from jinja2 import Environment, select_autoescape, FileSystemLoader

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
            elif isinstance(jsonobj[key], str):
                value = ""
            elif isinstance(jsonobj[key], unicode):
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
    print(output.encode('utf-8'), file=file_handle)


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
    with open(filename, mode='wb+') as file_handle:
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
            elif isinstance(sobject, time):
                fieldschema = bigquery.SchemaField(fname, 'TIME')
            elif isinstance(sobject, str) or isinstance(sobject, unicode):
                fieldschema = bigquery.SchemaField(fname, 'STRING')
            elif isinstance(sobject, bytes):
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


class DefaultCopyDriver(object):
    def pre_table_copy_filter(self, source_path):
        """
        Call back callled before copy a path is passed in project.dataset.table
        :param source_path: project.dataset.table  is format of string passed
        :return: Trues if the path is to be included in the copy
        """
        return True

    def pre_dataset_copy_filter(self, source_path):
        """
        Call back callled before copy a path is passed in project.dataset
        :param source_path: project.dataset.table  is format of string passed
        :return: Trues if the path is to be included in the copy
        """
        return True

    def destination_table_name(self, source_path, dstproject, dstdataset_name):
        """
        Function to calculate full path of destination table allows table name to be override.
        :param source_path:  a path to a source table i.e. project.dataset.table
        :param dstproject: a path to destination project this maybe identical to srcproject
        :param dstdataset_name:  a destination dataset maybe the sasme as src dataset
        :return: a path to the new table name in project.dataste.table format
        """
        assert dstproject is not None
        assert dstdataset_name is not None
        parts = source_path.split(".")
        return "{}.{}.{}@".format(dstproject, dstdataset_name, parts[2])

    def day_partition_deep_check(self):
        """

        :return: True if should check rows and bytes counts False if notrequired
        """
        return True

    def get_logger(self):
        """
        Returns the python logger to use for logging errors and issues
        :return:
        """
        return logging


def copy_bq_datset(srcproject_name, srcdataset_name, dstdataset_name, dstproject=None, maxday=None,
                   minday=None,
                   src_bucket=None, dst_bucket=None, copy_driver=None):
    return


def day_partition_by_partition_copy(srcproject_name, srcdataset_name, dstdataset_name, dstprojects=None,
                                    maxday=None, minday=None,
                                    srcBucket=None, dstBucklet=None, copyDriver=None):
    """
    A method totry andrepairday partitiontables.Startingpoint is srctable has anissue. So
    moving todestination.
    Tables must have matchingstructures and exist. use copy table jobs tocopy allpartitions.i.e
    srctable$yymmdd
    dsttable$yymmdd
    Errors logged but ignored as tring to recover data from corrupt table is the goal.
    :param srcproject_name: src project and pays for extracts and copies (if possible) must
    have job execution
    :param srcdataset_name: src tables dataset name
    :param dstdataset_name: destination table dataset name
    :param dstprojects: destination project pays for loads if not set src isassumed
    :param maxday: python datetime in utc use max day andworks back to min day orlast partition
    use max partition
    to workout min ifnone stat from today.
    :param minday: python datetime in utc use min as lastday
    :param srcBucket: when cross region is where objects are extracted too MUST be in same
    region as source dataset
    (9write access required)
    :param dstBucklet: when cross region where objects are copied too MUST be in same region
    9write access required)
    :param copyDriver: a copy driverclass this drives filtering of tables and depth of checking
    :return: Array of jobs
    """

    srcclient = bigquery.Client(())

    jobs = []

    if maxday is not None:
        maxday = maxday.replace(tzinfo=pytz.utc)
    if minday is not None:
        minday = minday.replace(tzinfo=pytz.utc)
    if dsttable_name is None:
        dsttable_name = srctable_name
    if srcproject is None:
        srcproject = self.tgtproject

    srcdataset_ref = client.dataset(srcdataset_name, project=srcproject)

    try:
        srcdataset = client.get_dataset(srcdataset_ref)
    except NotFound:
        copydriver.get_logger()().error(
            u"Day partion repair source data set:{} does not exist".format(
                srcdataset_name))
        return jobs

    srctable_ref = srcdataset.table(srctable_name)
    try:
        srctable = client.get_table(srctable_ref)
    except NotFound:
        copydriver.get_logger()().error(
            u"Day partion repair source table :{} does not exist".format(srctable_name))
        return jobs

    dstdataset_ref = client.dataset(dstdataset_name)
    try:
        dstdataset = client.get_dataset(dstdataset_ref)
    except NotFound:
        copydriver.get_logger()().error(
            u"Day partion repair source data set:{} does not exist".format(
                srcdataset_name))
        return jobs

    dsttable_ref = dstdataset.table(dsttable_name)
    try:
        dsttable = client.get_table(dsttable_ref)
    except NotFound:
        copydriver.get_logger()().error(
            u"Day partion repair source table :{} does not exist".format(srctable_name))
        return jobs

    # check tables and day partioned
    if srctable.table_type != 'TABLE' or dsttable.table_type != 'TABLE':
        copydriver.get_logger()().error(u"Src {}:{} or destination{}:{} is not a table".format(
            srctable_name,
            srctable.type, dsttable_name, dsttable.type))
        return jobs

    if srctable.partitioning_type is None or srctable.partitioning_type != 'DAY' or \
            dsttable.partitioning_type is None or dsttable.partitioning_type != 'DAY':
        copydriver.get_logger()().error(
            u"Src {}:{} or destination {}:{} is not a day partioned table".format(
                srctable_name,
                srctable.partitioning_type,
                dsttable_name,
                dsttable.partitioning_type))
    return jobs

# # this is not ok
# if srctable.modified > dsttable.modified:
#     copydriver.getLogger()().error(
#         u"Src {}:{} last modified after destination{}:{} ".format(srctable_name,
#                                                                   srctable.modified,
#                                                                   dsttable_name,
#                                                                   dsttable.modified))
#     return jobs
#
# # A wild assumption of schema match does not work
# # In reality destination MUST equal source
# # Given forseti is dynamic in this regard needtohandle
# srcschemaobj = self.genTemplateDict(srctable.schema)
# self.evolve_schema(srcschemaobj, dsttable, client=client)
#
# # get max partion tostart from
# if maxday is None or maxday > srctable.modified:
#     maxday = srctable.modified.replace(tzinfo=pytz.utc)
#     copydriver.getLogger()().info(
#         u"Using last modified on src table {}:{} as maxday to start at".format(
#             srctable_name, maxday))
#
#     srcpartionExpirationSecs = long(srctable.partition_expiration) / 1000
#     earliestStart = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(
#         seconds=srcpartionExpirationSecs)
# if minday is None or minday < earliestStart:
#     minday = earliestStart
#     copydriver.getLogger()().info(
#         u"Using last modified on src table - expiration {}:{} as minday to start at".format(
#             srctable_name,
#             minday))
#
# # never duplicate data solook atdates on target we are going to start at max
# # and work back so sind min and max and will start bfrommin back if maxday is greater
# dstquery = """#legacySQL
#  SELECT
#    MIN(_PARTITIONTIME) as minP
#  FROM
#    [{}:{}.{}]
#  ORDER BY 1 desc""".format(dsttable.project, dstdataset_name, dsttable_name)
#
# for row in self.runQuery(client, dstquery,
#                          "Detecting date range on dest table {} to avoid duplicating data".format(
#                              dsttable_name)):
#     minp = row["minP"]
#     if minp is not None and minp <= maxday:
#         copydriver.getLogger().info(
#             u"Using min time from destination table day partition {}:{} as maxday to start at as "
#             u"more than maxday {}".format(
#                 dsttable_name, minp, maxday))
#     # last partition loaded so start from 1 below
#     maxday = minp - timedelta(days=1)
#
# srcquery = """#legacySQL
#  SELECT
#    MIN(_PARTITIONTIME) as minP,
#    MAX(_PARTITIONTIME) as maxP
#  FROM
#    [{}:{}.{}]
#  WHERE _PARTITIONDATE <= TIMESTAMP("{}")
#  ORDER BY 1 desc""".format(dsttable.project, srcdataset_name, srctable_name, maxday.strftime(
#     "%Y-%m-%d 00:00:00.000 UTC"))
#
# hasdata = False
#
# # an exception here in BQ implies error
# # with table so fall back on human
# try:
#     for row in self.runQuery(client, srcquery,
#                              u"Detecting date range on dest table {} to avoid duplicating "
#                              u"data".format(
#                                  dsttable_name)):
#         hasdata = True
#         minp = row["minP"]
#         maxp = row["maxP"]
#         if minp is not None and minp > minday:
#             copydriver.getLogger()().info(
#                 u"Using min time from source table day partition {}:{} as maxday to start at as "
#                 u"more than maxday {}".format(
#                     srctable_name, minp, minday))
#             minday = minp
#         if maxp is not None and maxp < maxday:
#             copydriver.getLogger()().info(
#                 u"Using max time from source table day partition {}:{} as maxday to start at is "
#                 u"less than maxday {}".format(
#                     srctable_name, maxp, maxday))
#             maxday = maxp
#         if minp is None and maxp is None:
#             hasdata = False
#
#     except BQQueryError as e:
#     copydriver.getLogger().info(
#         u"Looks like src table  {}:{}.{} is corrupted relying on date ranges calculated so far {"
#         u"}: {}".format(
#             srctable.project, srcdataset_name, srctable_name, maxday, minday))
# hasdata = True
#
# if not hasdata:
#     copydriver.getLogger()().info(
#         u"No data in table skipping {}".format(
#             srctable_name))
# return jobs
#
# if maxday < minday:
#     copydriver.getLogger()().info(
#         u"Table {} maxday  {} less than min day {} nothing to do".format(
#             srctable_name, maxday, minday))
#     return jobs
#
# # do partition by partition copy
# copydriver.getLogger()().info(u"Creating {} day partion day jobs".format((maxday -
#                                                                           minday).days))
# jobsscheduled = 0
#
# currentday = maxday
# jobnum = 0
# while currentday >= minday:
#     jobnum = jobnum + 1
#     postfix = "$" + currentday.strftime("%Y%m%d")
#     currentday = currentday - timedelta(days=1)
#     jobsscheduled = jobsscheduled + 1
#     if jobsscheduled % 30 == 0:
#         copydriver.getLogger()().info(u"Created  jobs {} for source {}:{}.{}".format(
#             jobsscheduled, srcproject,
#             srcdataset_name, srctable_name))
#
#     # Cowardly donot overwrite destination partitions with data skip these
#     srcpartition_ref = srcdataset.table(srctable_name + postfix)
#     dstpartition_ref = dstdataset.table(dsttable_name + postfix)
#     srcpartition = client.get_table(srcpartition_ref)
#     dstpartition = client.get_table(dstpartition_ref)
#     job_config = bigquery.CopyJobConfig()
#     job_config.create_disposition = 'CREATE_IF_NEEDED'
#     job_config.write_disposition = 'WRITE_APPEND'
#     copy_job = client.copy_table(
#         srcpartition, dstpartition, job_config=job_config)
#
#     jobs.append({"job": {"id": copy_job, "description": "Post Job for repair from table:{
#                          }".format(srcpartition.table_id)}})
#     if (jobnum % 7) == 0:
#         self.waitForBQJobs(jobs)
#     jobs = []
#
#     # return jobs aslist
#     # Allows lotstobequeuedand then just wait forjobs to end
#     # Should be reasonable for lots of tables
# return jobs
