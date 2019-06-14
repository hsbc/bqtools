from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from jinja2 import Environment, select_autoescape, FileSystemLoader, TemplateNotFound
import re
import os


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
                raise Exception("Oops")
    return template

def clean_json_for_bq(anobject):
    """

    :param object to be converted to big query json compatible format:
    :return: cleaned object
    """
    newobj = {}
    if not isinstance(anobject, dict):
        raise Exception("Oops")
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

    :param filename: filename to putput too
    :param resourcelist: list of resources to genereate code for
    :return:nothing
    """
    with open(filename, mode='wb+') as fh:
        generate_create_schema(resourcelist, fh)


