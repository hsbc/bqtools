from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from jinja2 import Environment, select_autoescape, FileSystemLoader, TemplateNotFound
from datetime import datetime, date, timedelta, time
import re
import os
import json
from google.cloud import bigquery
import pprint
import logging
import copy


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

    CUSTOM_ERROR_MESSAGE = 'The json structure passed has inconsistent types for the same key {0} and thus cannot be made into a BQ schema or valid json new line for loading onto big query\n{1} type previously {2}'

    def __init__(self, resource_name, e,ttype):
        super(InconsistentJSONStructure, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e, ttype))

class NotADictionary(BQTError):
    CUSTOM_ERROR_MESSAGE = 'The json structure passed is not a dictionary\n{1}'
    def __init__(self, resource_name, e):
        super(NotADictionary, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e))

class UnexpectedType(BQTError):
    CUSTOM_ERROR_MESSAGE = 'The object type of \n{1} is nota type that bqutils knows how to handle'
    def __init__(self, resource_name, e):
        super(UnexpectedType, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name, e))

class UnexpectedDict(BQTError):
    CUSTOM_ERROR_MESSAGE = 'The object is a dict and shoulld not be \n{0}'
    def __init__(self, resource_name):
        super(UnexpectedDict, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(resource_name))

class SchemaMutationError(BQTError):
    """Error for API executions."""

    CUSTOM_ERROR_MESSAGE = 'Schema Mutation Error: unable to mutate object path {0} on keyi {1} object {2}'

    def __init__(self, objtomatch, keyi, path):
        super(SchemaMutationError, self).__init__(
            self.CUSTOM_ERROR_MESSAGE.format(objtomatch,keyi,path))

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
                value = [{}]
                if len(jsonobj[key]) > 0:
                    if not isinstance(jsonobj[key][0],dict):
                        nv = []
                        for vali in jsonobj[key]:
                            nv.append({"value":vali})
                        jsonobj[key]=nv
                    for li in jsonobj[key]:
                        value[0] = get_json_struct(li,value[0])
            else:
                raise UnexpectedType(str(jsonobj[key]))
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
                            template[newkey][0] = get_json_struct(li, template[newkey][0])
            else:
                raise InconsistentJSONStructure(key,str(jsonobj[key]),str(template[newkey]))
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

def dataset_exists( client, dataset_reference):
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

def table_exists( client, table_reference):
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

def create_schema(sobject, sd=0, fname=None,dschema=None):

    schema = []
    if dschema is None:
        dschema = {}
    dummyfield = bigquery.SchemaField('xxxDummySchemaAsNoneDefinedxxx', 'STRING')

    if fname is not None:
        fname = INVALIDBQFIELDCHARS.sub("_",fname)
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
                tschema.extend(create_schema(i,dschema=pdschema))
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
                    fieldschema = create_schema(sobject[j], fname=j,dschema=dschema[j])
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
            elif (isinstance(sobject, int)):
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
                    fieldschema.fields = create_schema(head, sd + 1)
            elif isinstance(sobject, dict):
                fieldschema = bigquery.SchemaField(fname, 'RECORD')
                fieldschema.fields = create_schema(sobject, sd + 1)
            else:
                raise UnexpectedType(str(type(sobject)))
            if dschema is not None:
                dschema["simple"]=True
            return ([fieldschema])
        else:
            return ([])

    return (schema)

# convert a dict and with a schema object to assict convert dict into tuple
def dict_plus_schema_2_tuple(data, schema):
    """
    
    :param data: 
    :param schema: 
    :return: 
    """
    otuple = []

    # must  iterate through schema to add Nones so dominates
    for si in schema:
        value = None
        if data is not None and si.name in data:
            value = data[si.name]
        if si.field_type == 'RECORD':
            ttuple = []
            if si.mode != 'REPEATED' or value is None:
                value = [value]
            for vi in value:
                value = dict_plus_schema_2_tuple(vi, si.fields)
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
    for si, value in zip(schema, data):
        if si.field_type == 'RECORD':
            ldata = []
            if si.mode == 'REPEATED':
                llist = value
            else:
                llist = [value]
            for li in llist:
                ldata.append(tuple_plus_schema_2_dict(li, si.fields))
            if si.mode == 'REPEATED':
                value = ldata
            else:
                value = ldata[0]
        rdata[si.name] = value

    return rdata

def gen_template_dict(schema):
    """

    :param schema: Take a rest representation of google big query table fields and create a template json object
    :return: 
    """
    rdata = {}
    for si in schema:
        value = None
        if si.field_type == 'RECORD':
            tvalue = self.gen_template_dict(si.fields)
            if si.mode == 'REPEATED':
                value = [tvalue]
            else:
                value = tvalue
        elif si.field_type == 'INTEGER':
            value = 0
        elif si.field_type == 'BOOLEAN':
            value = False
        elif si.field_type == 'FLOAT':
            value = 0.0
        elif si.field_type == 'STRING':
            value = ""
        elif si.field_type == 'DATETIME':
            value = datetime.utcnow()
        elif si.field_type == 'DATE':
            value = date.today()
        elif si.field_type == 'TIME':
            value = datetime.time()
        elif si.field_type == 'BYTES':
            value = b'\x00'
        else:
            raise UnexpectedType(str(type(sobject)))
        rdata[si.name] = value

    return rdata

def to_dict( schema):
    me = {"name": schema.name,
          "type": schema.field_type,
          "description": schema.description,
          "mode": schema.mode,
          "fields": None}
    if schema.fields is not None:
        fa = []
        for fi in schema.fields:
            fa.append(to_dict(fi))
        me['fields'] = fa
    return (me)

def calc_field_depth(fieldlist, depth=0):
    md = depth
    rd = depth
    for i in fieldlist:
        if 'fields' in i:
            rd = calc_field_depth(i['fields'], depth + 1)
            if rd > md:
                md = rd
    return(md)

def trunc_field_depth(fieldlist, maxdepth, depth=0):
    nf = []
    if depth <= maxdepth:
        for i in fieldlist:
            nf.append(i)
            if 'fields' in i:
                if depth == maxdepth:
                    # json.JSONEncoder().encode(fieldlist)
                    i['type']='STRING'
                    i.pop('fields', None)
                else:
                    i['fields'] = trunc_field_depth(
                        i['fields'], maxdepth, depth + 1)

    return(nf)

def match_and_addtoschema(objtomatch, schema, evolved=False, path="",logger=None):

    pp = pprint.PrettyPrinter(indent=4)
    poplist = {}

    for keyi in objtomatch:
        # Create schema does this adjustment so we need to do same in actual object
        thekey = INVALIDBQFIELDCHARS.sub('_',keyi)
        # Work out if object keys have invalid values and n
        if thekey != keyi:
            poplist[keyi]=thekey
        matchstruct = False
        # look for bare list should not have any if known about
        # big query cannot hande bare lists
        # so to alow schema evoution MUST be removed
        # this test if we have a list and a value in it is it a bare type i.e.not a dictionary
        # if it is not a dictionary use bare type ist method to cnvert to a dictionary
        # where object vallue is a singe key in a dict of value
        # this changes each object as well meaning they will load into the evolved schema
        # we call this with log error false as this method checks if the key exists and
        # if the object is a list and lengh > 0 and if the object at the end is dict or not only converts if not a dict
        # this is important optimisation as if we checked here it would be a double check
        # as lots of objects this overhead is imprtant to minimise hence why this
        # looks like it does
        do_bare_type_list(objtomatch,keyi,"value")
        for si in schema:
            if thekey == si.name:
                if si.field_type == 'RECORD':
                    if si.mode == 'REPEATED':
                        subevolve = evolved
                        for listi in objtomatch[keyi]:
                            # TODO hack to modify fields as .fields is immutable since version 0.28 and later but not in docs!!
                            si._fields = list(si.fields)
                            tsubevolve = match_and_addtoschema(listi, si.fields, evolved=evolved,
                                                                    path=path + "." + thekey)
                            if not subevolve and tsubevolve:
                                subevolve = tsubevolve
                        evolved = subevolve
                    else:
                        # TODO hack to modify fields as .fields is immutable since version 0.28 and later but not in docs!!
                        si._fields = list(si.fields)
                        evolved = match_and_addtoschema(objtomatch[keyi], si.fields, evolved=evolved)
                matchstruct = True
                break
        if matchstruct:
            continue

        # Construct addition to schema here based on objtomatch[keyi] schema or object type
        # append to the schema list
        try:
            toadd = create_schema(objtomatch[keyi], fname=keyi)
        except Exception as e:
            raise SchemaMutationError(str(objtomatch),keyi,path)

        if toadd is not None:
            schema.extend(toadd)
            if logger is not None:
                logger.warning(
                     u"Evolved path = {}, struct={}".format(path + "." + thekey, pp.pformat(objtomatch[keyi])))
            evolved = True

    # If values of keys did need changing change them
    if len(poplist):
        for pi in poplist:
            objtomatch[poplist[pi]]=objtomatch[pi]
            objtomatch.pop(pi,None)


    return evolved

def do_bare_type_list(adict, key, detail,logger=None):
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
        key: The key name of the list if it does not exist this does nothing. if the item at the key is not a list it does nothing if length of list is 0 this does nothing
        detail: The name of the field in new sub dictionary of each object
        logerror: boolean if true and list objects are already a dictionary will log trace and key that ha dthe issue


    Returns:
        Nothing.

    Raises:
        Nothing
    """
    try:
        if key in adict:
            if key in adict and isinstance(adict[key],list) and len(adict[key]) > 0:
                if not isinstance(adict[key][0],dict):
                    nl = []
                    for li in adict[key]:
                        nl.append({detail:li})
                    adict[key] = nl
                else:
                    if logger is not None:
                        tbs = traceback.extract_stack()
                        tbsflat = "\n".join(map(str, tbs))
                        logger.error("Bare list for key {} in dict {} expected a basic type not converting {}".format(key,str(adict),tbsflat))
    except Exception as e:
        raise UnexpectedDict("Bare list for key {} in dict {} expected a basic type not converting".format(key,str(adict)))

def recurse_and_add_to_schema(schema, oschema):
    changes = False

    # Minimum is new schema now this can have less than old
    wschema = copy.deepcopy(schema)

    # Everything in old schema stays as a patch
    for oi in oschema:
        nschema = []
        # Look for
        for ns in wschema:
            if oi['name'].lower() == ns.name.lower():
                if oi['type'] == 'RECORD':
                    rchanges, oi['fields'] = recurse_and_add_to_schema(ns.fields, oi['fields'])
                    if rchanges == True and changes == False:
                        changes = rchanges
            else:
                nschema.append(ns)
        wschema = nschema

    # Now just has what remain in it.
    for wsi in wschema:
        changes = True
        oschema.append(to_dict(wsi))

    return (changes, oschema)

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
    :param table: the base table to do diffs (assumes each time slaice is a view of what data looked like))
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
    if isinstance(fieldsappend,list):
        for fdiffi in fieldsappend:
            fieldsnot4diff.append(fdiffi)
    if update_only_fields is None:
        update_only_fields= ['creationTime',
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
    fieldsUpdateOnly = []
    aliasnum = 1

    basedata = {"select": baseselectclause, "from": basefromclause, "aliasnum": aliasnum}

    def recurseDiffbase(schema, fieldprefix, curtablealias):
        pp = pprint.PrettyPrinter(indent=4)

        for i in schema:
            skip = False
            for fndi in fieldsnot4diff:
                if i.name == fndi:
                    skip = True
                    break
            if skip:
                continue
            if i.field_type == 'STRING':
                basefield = ',\n    ifnull({}.{},"None") as {}'.format(
                    curtablealias,
                    i.name,
                    fieldprefix + i.name)
            elif i.field_type == 'BOOLEAN':
                basefield = ',\n    ifnull({}.{},False) as {}'.format(curtablealias, i.name, fieldprefix + i.name)
            elif i.field_type == 'INTEGER':
                basefield = ',\n    ifnull({}.{},0) as {}'.format(curtablealias, i.name, fieldprefix + i.name)
            elif i.field_type == 'FLOAT':
                basefield = ',\n    ifnull({}.{},0.0) as {}'.format(curtablealias, i.name, fieldprefix + i.name)
            elif i.field_type == 'DATE':
                basefield = ',\n    ifnull({}.{},DATE(1970,1,1)) as {}'.format(curtablealias, i.name,
                                                                               fieldprefix + i.name)
            elif i.field_type == 'DATETIME':
                basefield = ',\n    ifnull({}.{},DATETIME(1970,1,1,0,0,0)) as {}'.format(curtablealias, i.name,
                                                                                         fieldprefix + i.name)
            elif i.field_type == 'TIME':
                basefield = ',\n    ifnull({}.{},TIME(0,0,0)) as {}'.format(curtablealias, i.name,
                                                                            fieldprefix + i.name)
            elif i.field_type == 'BYTES':
                basefield = ',\n    ifnull({}.{},b"\x00") as {}'.format(curtablealias, i.name, fieldprefix + i.name)
            elif i.field_type == 'RECORD':
                aliasstack.append(curtablealias)
                fieldprefixstack.append(fieldprefix)
                fieldprefix = fieldprefix + i.name
                if i.mode == 'REPEATED':
                    oldalias = curtablealias
                    curtablealias = "A{}".format(basedata['aliasnum'])
                    basedata['aliasnum'] = basedata['aliasnum'] + 1

                    basedata['from'] = basedata['from'] + "\nLEFT JOIN UNNEST({}) as {}".format(
                        oldalias + "." + i.name, curtablealias)

                else:
                    curtablealias = curtablealias + "." + i.name
                recurseDiffbase(i.fields, fieldprefix, curtablealias)
                curtablealias = aliasstack.pop()
                fieldprefix = fieldprefixstack.pop()
                continue
            updateOnly = False
            for fndi in update_only_fields:
                if i.name == fndi:
                    updateOnly = True
                    break
            if updateOnly:
                fieldsUpdateOnly.append(fieldprefix + i.name)
            else:
                fields4diff.append(fieldprefix + i.name)
            basedata['select'] = basedata['select'] + basefield
        return

    recurseDiffbase(schema, fieldprefix, curtablealias)
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
""".format(refbasediffview, time_expr, project, dataset,table)

    for f4i in fields4diff:
        diffieldclause = diffieldclause + ",\n    o.{} as orig{},\n    l.{} as later{},\n    case when o.{} = l.{} then 0 else 1 end as diff{}".format(
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

    for f4i in fieldsUpdateOnly:
        diffieldclause = diffieldclause + ",\n    o.{} as orig{},\n    l.{} as later{},\n    case when o.{} = l.{} then 0 else 1 end as diff{}".format(
            f4i, f4i, f4i, f4i, f4i, f4i, f4i)
        diffcaseclause = diffcaseclause + "AND o.{} = l.{} ".format(f4i, f4i)

    diffcaseclause = diffcaseclause + """THEN 'Same'
    ELSE 'Updated'
  END AS action"""

    for intervali in intervals:
        for keyi in intervali:
            viewname = table + "diff" + keyi
            viewdescription = "Diff of {} of underlying table {} description: {}".format(keyi, table, description)
            views.append({"name": viewname,
                          "query": diffviewselectclause + diffcaseclause + diffieldclause + diffviewfromclause.replace(
                              "%interval%", intervali[keyi]) + diffwhereclause, "description": viewdescription})

    ## look for id in top level fields if exists create first seen and last seen views
    for i in schema:
        if i.name == "id":
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
            FSLSTDT = "View that shows {} captured values of underlying table for object of a given non repeating key of 'id' {}.{}.{}"
            fsv = FSLST.format(project,dataset,table,"firstSeenTime",time_expr)
            fsd = FSLSTDT.format("first",project, dataset, table)
            lsv = FSLST.format(project, dataset, table, "lastSeenTime",time_expr)
            lsd = FSLSTDT.format("last", project, dataset, table)
            views.append({"name": table+"fs",
                          "query": fsv , "description": fsd})
            views.append({"name": table + "ls",
                          "query": lsv, "description": lsd})
            break


    return views

def evolve_schema(insertobj, table, client,bigquery,logger=None):
    """

    :param insertobj: json object that represents schema expected
    :param table: a table object from python api thats been git through client.get_table
    :param client: a big query client object
    :param bigquery: big query service as created with google discovery discovery.build("bigquery","v2")
    :param logger: a google logger class
    :return: evolved True or False
    """

    schema = list(table.schema)
    tablechange=False

    evolved = match_and_addtoschema(insertobj, schema)

    if evolved:
        if logger is not None:
            logger.warning(
                u"Evolving schema as new field(s) on {}:{}.{} views with * will need reapplying".format(
                    table.project, table.dataset_id, table.table_id))

        treq = bigquery.tables().get(projectId=table.project, datasetId=table.dataset_id, tableId=table.table_id)
        tableData = treq.execute()
        oschema = tableData.get('schema')
        tablechange, pschema = recurse_and_add_to_schema(schema, oschema['fields'])
        update = {'schema': {"fields": pschema}}
        preq = bigquery.tables().patch(projectId=table.project, datasetId=table.dataset_id,
                                       tableId=table.table_id,
                                       body=update)
        preq.execute()
        client.get_table(table)
        # table.reload()

    return evolved

class ViewCompiler(object):
    def __init__(self):
        self.view_depth_optimiser=[]

    def compile(self, dataset, name, sql):

        standardSQL = True
        compiledSQL = sql
        prefix = ""
        if sql.strip().lower().find(u"#standardsql") == 0:
            prefix = u"#standardSQL\n"
        else:
            standardSQL = False
            prefix = u"#legacySQL\n"

        # get rid of nested comments as they can break this even if in a string in a query
        prefix = prefix + """
-- ===================================================================================
-- 
--                             ViewCompresser Output
--     
--                      \/\/\/\/\/\/Original SQL Below\/\/\/\/
"""
        for line in sql.splitlines():
            prefix = prefix + "-- " + line + "\n"
        prefix = prefix + """
-- 
--                      /\/\/\/\/\/\Original SQL Above/\/\/\/\
--                      
--                            Compiled SQL below
-- ===================================================================================
"""
        for i in self.view_depth_optimiser:
            # relaces a table or view name with sql
            if standardSQL == False:
                compiledSQL = compiledSQL.replace(
                    "[" + i + "]",
                    "( /* flattened view [-" + i + "-]*/ " + self.view_depth_optimiser[i]['unnested'] + ")")
            else:
                compiledSQL = compiledSQL.replace(
                    "`" + i.replace(':', '.') + "`",
                    "( /* flattened view `-" + i + "-`*/ " + self.view_depth_optimiser[i]['unnested'] + ")")

        self.view_depth_optimiser[dataset.project + ":" + dataset.dataset_id + "." + name] = {"raw": sql,
                                                                                                "unnested": compiledSQL}

        # look to keep queriesbelow maximumsize
        if len(prefix + compiledSQL) > 256000:
            # strip out comment
            if standardSQL:
                prefix="#standardSQL\n"
            else:
                prefix="#legacySQL\n"
            # if still too big strip out other comments
            # and extra space
            if len(prefix + compiledSQL) > 256000:
                nsql = ''
                for line in compiledSQL.split("\n").trim():
                    # if not a comment
                    if line[:2] != "--":
                        ' '.join(line.split())
                        nsql = nsql + "\n" + line
                compiledSQL = nsql

                # if still too big go back to original sql stripped
                if len(prefix + compiledSQL) > 256000:
                    if len(sql) > 256000:
                        nsql = ''
                        for line in origsql.split("\n").trim():
                            # if not a comment
                            if line[:1] != "#":
                                ' '.join(line.split())
                                nsql = nsql + "\n" + line
                                compiledSQL = nsql
                    else:
                        compiledSQL = sql

        return (prefix + compiledSQL)