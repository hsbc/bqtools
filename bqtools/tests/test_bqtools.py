# -*- coding: utf-8 -*-
"""
This modules purpose is to test bqtools-json

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import copy
import datetime
import difflib
import json
import logging
import pprint
import unittest

import pytz
from deepdiff import DeepDiff
from google.cloud import bigquery, storage, exceptions

import bqtools


class MockDataset:
    def __init__(self,project,dataset):
        self._dataset = dataset
        self._project = project
    @property
    def dataset_id(self):
        return self._dataset

    @property
    def project(self):
        return self._project

class TestScannerMethods(unittest.TestCase):
    def load_data(self, file_name):
        with open(file_name) as json_file:
            return json.load(json_file)

    def setUp(self):

        logging.basicConfig()

        self.pp = pprint.PrettyPrinter(indent=4)
        # test 1 validate can create a schema from a dictionary
        self.schemaTest1 = self.load_data("bqtools/tests/schemaTest1.json")
        self.schemaTest2 = self.load_data("bqtools/tests/schemaTest2.json")

        # next  schemas are for testing bare array handling
        # this is a starting schema
        self.schema2startnobare = self.load_data("bqtools/tests/schema2startnobare.json")

        # this adds 2 bare arrays
        self.schemaTest2bare = self.load_data("bqtools/tests/schemaTest2bare.json")

        # resultant schema and objects shoulld loook like this
        self.schemaTest2nonbare = self.load_data("bqtools/tests/schemaTest2nonbare.json")

        self.schemaTest4 = self.load_data("bqtools/tests/schemaTest4.json")

        self.schemaTest3 = self.load_data("bqtools/tests/schemaTest3.json")
        self.monsterSchema = self.load_data("bqtools/tests/monsterSchema.json")

    def test_toDict(self):
        schema2Dict = (
            bigquery.SchemaField('string', 'STRING'),
            bigquery.SchemaField('integer', 'INTEGER'),
            bigquery.SchemaField('float', 'FLOAT'),
            bigquery.SchemaField('boolean', 'BOOLEAN'),
            bigquery.SchemaField('record', 'RECORD', fields=(
                bigquery.SchemaField('string2', 'STRING'),
                bigquery.SchemaField('float', 'FLOAT'),
                bigquery.SchemaField('integer2', 'INTEGER'),
                bigquery.SchemaField('boolean2', 'BOOLEAN')
            )),
            bigquery.SchemaField('array', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('string3', 'STRING'),
                bigquery.SchemaField('integer3', 'INTEGER')
            ))
        )

        expectedResult = [
            {
                "name": 'string',
                "type": 'STRING',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'integer',
                "type": 'INTEGER',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'float',
                "type": 'FLOAT',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'boolean',
                "type": 'BOOLEAN',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'record',
                "type": 'RECORD',
                "description": None,
                "mode": 'NULLABLE',
                "fields": [
                    {"name": 'string2',
                     "type": 'STRING',
                     "description": None,
                     "mode": 'NULLABLE',
                     "fields": []},
                    {
                        "name": 'float',
                        "type": 'FLOAT',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []},
                    {
                        "name": 'integer2',
                        "type": 'INTEGER',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []},
                    {
                        "name": 'boolean2',
                        "type": 'BOOLEAN',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []}
                ]},
            {
                "name": 'array',
                "type": 'RECORD',
                "description": None,
                "mode": 'REPEATED',
                "fields": [
                    {"name": 'string3',
                     "type": 'STRING',
                     "description": None,
                     "mode": 'NULLABLE',
                     "fields": []},
                    {
                        "name": 'integer3',
                        "type": 'INTEGER',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []}
                ]}
        ]
        sa = []

        # print("showing each field")
        for bqi in schema2Dict:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)
        diff = DeepDiff(expectedResult, sa, ignore_order=True)
        self.assertEqual(diff, {},
                         "Unexpected result in toDict expected nothing insteadest got {}".format(
                             self.pp.pprint(diff)))

    def test_createschema(self):
        bqSchema = bqtools.create_schema(self.schemaTest1)
        expectedSchema = (
            bigquery.SchemaField('string', 'STRING'),
            bigquery.SchemaField('integer', 'INTEGER'),
            bigquery.SchemaField('float', 'FLOAT'),
            bigquery.SchemaField('boolean', 'BOOLEAN'),
            bigquery.SchemaField('record', 'RECORD', fields=(
                bigquery.SchemaField('string2', 'STRING'),
                bigquery.SchemaField('float', 'FLOAT'),
                bigquery.SchemaField('integer2', 'INTEGER'),
                bigquery.SchemaField('boolean2', 'BOOLEAN')
            )),
            bigquery.SchemaField('array', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('string3', 'STRING'),
                bigquery.SchemaField('integer3', 'INTEGER')
            ))
        )

        # print("testing result")
        # self.pp.pprint(bqSchema)
        sa = []

        # print("showing each field")
        for bqi in bqSchema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)
        # print("Schema as dict")
        # self.pp.pprint(sa)
        isa = sa

        # print("Expected result")
        # self.pp.pprint(expectedSchema)
        sa = []

        # print("showing each expected field")
        for bqi in expectedSchema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)
        # print("expected Schema as dict")
        diff = DeepDiff(isa, sa, ignore_order=True)
        # self.pp.pprint(diff)
        a = "Schema test1 schema does not match target {}".format(len(diff))
        self.assertEqual(diff, {}, a)

    def test_createschema2(self):
        # print("Creating a new schema")

        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        sa2 = []

        # print("showing each field schema2")
        for bqi in bqSchema2:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)
        # print("Schema2 as dict")
        # self.pp.pprint(sa2)
        expectedSchema2 = (
            bigquery.SchemaField('string', 'STRING'),
            bigquery.SchemaField('integer', 'INTEGER'),
            bigquery.SchemaField('record', 'RECORD', fields=(
                bigquery.SchemaField('string2', 'STRING'),
                bigquery.SchemaField('float', 'FLOAT'),
                bigquery.SchemaField('boolean2', 'BOOLEAN'),
                bigquery.SchemaField('appended1', 'STRING')
            )),
            bigquery.SchemaField('array', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('string3', 'STRING'),
                bigquery.SchemaField('integer3', 'INTEGER'),
                bigquery.SchemaField('foo', 'FLOAT')
            )),
            bigquery.SchemaField('anotherarray', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('test1', 'INTEGER'),
                bigquery.SchemaField('test2', 'BOOLEAN')
            ))
        )
        sa = []
        for bqi in expectedSchema2:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)
        # self.pp.pprint(diff)
        a = "Schema test1 schema does not match target {}".format(diff)
        self.assertEqual(diff, {}, a)
        logger = logging.getLogger("testBQTools")

        evolved = bqtools.match_and_addtoschema({"string": "hello"}, expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 1")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52}, expectedSchema2,
                                                logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 2")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52, "record": {}},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 3")
        evolved = bqtools.match_and_addtoschema(
            {"string": "hello", "integer": 52, "record": {"string2": "hello2"}}, expectedSchema2,
            logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 4")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"}},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 6")

        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": []},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 7")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello"}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 8")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 9")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 10")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 11")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False},
                                                                  {"test1": 52, "test2": True}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 12")

        # evolve tests bbelow prepare baseline
        copyoforigschema = list(expectedSchema2)
        savedSchema = copy.deepcopy(copyoforigschema)

        sa = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)

        # Evolutio test 1
        # add some stuff 2 layers down in an array
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False},
                                                                  {"test1": 52, "test2": True,
                                                                   "fred": "I am an evolved string",
                                                                   "iamanotherevolve": 32}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 13")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)
        diff = dict(diff)

        print(
            "============================================ evolve test 1 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 1 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolve',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected {}".format(self.pp.pformat(diff)))
        # Evolution test 2
        #  this just adds a fiedl at top level
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False},
                                                                  {"test1": 52, "test2": True}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 2 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 2 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}}}, diff,
                         "Schema evolution not as expected {}".format(self.pp.pformat(diff)))

        # Evolution test 3
        # this is an object with root schema evolution
        # Plus child objects with 2 different changes in them
        # plus another with both
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False,
                                                                   "fred": "I am an evolution"},
                                                                  {"test1": 52, "test2": True,
                                                                   "iamanotherevolution": 1.3},
                                                                  {"test1": 52, "test2": True,
                                                                   "iamanotherevolution": 1.3,
                                                                   "fred": "I am same previous "
                                                                           "evolution"}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 3 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 3 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected {}".format(self.pp.pformat(diff)))
        # Evolution test 4
        # this is an object with root schema evolution
        # Plus child objects with 2 different changes in them
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [
                                                     {"test1": 52, "test2": False,
                                                      "fred": "I am an evolution"},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 4 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 4 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected")
        # Evolution test 5
        # add an array with strings an dno key this should fail
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [
                                                     {"test1": 52, "test2": False,
                                                      "fred": "I am an evolution"},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3,
                                                      "bill": ["hello", "fred", "break this"]}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 5 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 5 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'},
                                                                         {'description': None,
                                                                          'fields': [
                                                                              {'description': None,
                                                                               'fields': [],
                                                                               'mode': 'NULLABLE',
                                                                               'name': 'value',
                                                                               'type': 'STRING'}],
                                                                          'mode': 'REPEATED',
                                                                          'name': 'bill',
                                                                          'type': 'RECORD'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected")

        # Evolution test 6
        # add an array with strings an dno key this should fail
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [
                                                     {"test1": 52, "test2": False,
                                                      "fred": "I am an evolution"},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3,
                                                      "bill": {}}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 6 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 6 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'},
                                                                         {'description': None,
                                                                          'fields': [
                                                                              {'description': None,
                                                                               'fields': [],
                                                                               'mode': 'NULLABLE',
                                                                               'name':
                                                                                   'xxxDummySchemaAsNoneDefinedxxx',
                                                                               'type': 'STRING'}],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'bill',
                                                                          'type': 'RECORD'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected")

    # def test_patchbare(self):
    #     startschema = bqtools.create_schema(self.schema2startnobare)
    #     resultschema = bqtools.create_schema(self.schemaTest2nonbare)
    #
    #     origobject = copy.deepcopy(self.schemaTest2bare)
    #
    #     evolved = bqtools.match_and_addtoschema(self.schemaTest2bare, startschema)
    #     self.assertEqual(evolved, True,
    #                      "Bare llist and multi dict evolution has not happened as expected")
    #     diff = DeepDiff(resultschema, startschema, ignore_order=True)
    #
    #     print(
    #         "============================================ mixed arrays added  diff start  "
    #         "====================================")
    #     print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
    #     print(
    #         "============================================ mixed arrays added  diff end  "
    #         "====================================")



    def test_patch(self):

        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        bqSchema = bqtools.create_schema(self.schemaTest1)

        sa = []

        for bqi in bqSchema:
            i = bqtools.to_dict(bqi)
            sa.append(i)

        osa = copy.deepcopy(sa)

        change, pschema = bqtools.recurse_and_add_to_schema(bqSchema2, sa)
        diff = DeepDiff(pschema, osa, ignore_order=True)

        # patching never removes fields so expect additions
        # so after list of root[] should be one longer
        expectedDiff = {'iterable_item_added': {'root[2]': {'description': None,
                                                            'fields': [{'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'integer2',
                                                                        'type': 'INTEGER'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'float',
                                                                        'type': 'FLOAT'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'string2',
                                                                        'type': 'STRING'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'boolean2',
                                                                        'type': 'BOOLEAN'}],
                                                            'mode': 'NULLABLE',
                                                            'name': 'record',
                                                            'type': 'RECORD'},
                                                'root[5]': {'description': None,
                                                            'fields': [{'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'integer3',
                                                                        'type': 'INTEGER'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'string3',
                                                                        'type': 'STRING'}],
                                                            'mode': 'REPEATED',
                                                            'name': 'array',
                                                            'type': 'RECORD'}},
                        'iterable_item_removed': {'root[2]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'integer2',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'float',
                                                                          'type': 'FLOAT'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'string2',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'boolean2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'appended1',
                                                                          'type': 'STRING'}],
                                                              'mode': 'NULLABLE',
                                                              'name': 'record',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'integer3',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'string3',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'foo',
                                                                          'type': 'FLOAT'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'array',
                                                              'type': 'RECORD'},
                                                  'root[6]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'}}}

        self.assertEqual(diff, expectedDiff,
                         "Patch diff is not what is expected {}".format(self.pp.pformat(diff)))
        self.assertEqual(change, True,
                         "Patch diff change result {} is not what is expected {}".format(change,
                                                                                         self.pp.pformat(
                                                                                             diff)))

        bqSchema3 = bqtools.create_schema(self.schemaTest3)
        bqSchema4 = bqtools.create_schema(self.schemaTest4)

        sa2 = []

        for bqi in bqSchema3:
            i = bqtools.to_dict(bqi)
            sa2.append(i)

        osa = copy.deepcopy(sa2)

        change, pschema = bqtools.recurse_and_add_to_schema(bqSchema4, sa2)
        diff = DeepDiff(pschema, osa, ignore_order=True)
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), change))

    #        print("old {}".format(self.pp.pformat(osa)))
    #        print("new {}".format(self.pp.pformat(pschema)))

    def test_patch2(self):

        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        bqSchema = bqtools.create_schema(self.schemaTest2)

        sa = []

        for bqi in bqSchema:
            i = bqtools.to_dict(bqi)
            sa.append(i)

        osa = copy.deepcopy(sa)

        change, pschema = bqtools.recurse_and_add_to_schema(bqSchema2, sa)
        diff = DeepDiff(pschema, osa, ignore_order=True)

        # patching never removes fields so expect additions
        # so after list of root[] should be one longer
        expectedDiff = {}

        self.assertEqual(diff, expectedDiff,
                         "Patch diff is not what is expected {}".format(self.pp.pformat(diff)))

        self.assertEqual(change, False,
                         "Patch diff change result {} is not what is expected {}".format(change,
                                                                                         self.pp.pformat(
                                                                                             diff)))

    #        print("Patched schema diff {}".format(self.pp.pformat(diff)))
    #        print("old {}".format(self.pp.pformat(osa)))
    #        print("new {}".format(self.pp.pformat(pschema)))

        # resultant schema and objects shoulld loook like this
        self.schemaTest2nonbare = self.load_data("bqtools/tests/schemaTest2nonbare.json")

        self.schemaTest4 = self.load_data("bqtools/tests/schemaTest4.json")

        self.schemaTest3 = self.load_data("bqtools/tests/schemaTest3.json")
        self.monsterSchema = self.load_data("bqtools/tests/monsterSchema.json")

    def test_toDict(self):
        schema2Dict = (
            bigquery.SchemaField('string', 'STRING'),
            bigquery.SchemaField('integer', 'INTEGER'),
            bigquery.SchemaField('float', 'FLOAT'),
            bigquery.SchemaField('boolean', 'BOOLEAN'),
            bigquery.SchemaField('record', 'RECORD', fields=(
                bigquery.SchemaField('string2', 'STRING'),
                bigquery.SchemaField('float', 'FLOAT'),
                bigquery.SchemaField('integer2', 'INTEGER'),
                bigquery.SchemaField('boolean2', 'BOOLEAN')
            )),
            bigquery.SchemaField('array', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('string3', 'STRING'),
                bigquery.SchemaField('integer3', 'INTEGER')
            ))
        )

        expectedResult = [
            {
                "name": 'string',
                "type": 'STRING',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'integer',
                "type": 'INTEGER',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'float',
                "type": 'FLOAT',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'boolean',
                "type": 'BOOLEAN',
                "description": None,
                "mode": 'NULLABLE',
                "fields": []},
            {
                "name": 'record',
                "type": 'RECORD',
                "description": None,
                "mode": 'NULLABLE',
                "fields": [
                    {"name": 'string2',
                     "type": 'STRING',
                     "description": None,
                     "mode": 'NULLABLE',
                     "fields": []},
                    {
                        "name": 'float',
                        "type": 'FLOAT',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []},
                    {
                        "name": 'integer2',
                        "type": 'INTEGER',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []},
                    {
                        "name": 'boolean2',
                        "type": 'BOOLEAN',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []}
                ]},
            {
                "name": 'array',
                "type": 'RECORD',
                "description": None,
                "mode": 'REPEATED',
                "fields": [
                    {"name": 'string3',
                     "type": 'STRING',
                     "description": None,
                     "mode": 'NULLABLE',
                     "fields": []},
                    {
                        "name": 'integer3',
                        "type": 'INTEGER',
                        "description": None,
                        "mode": 'NULLABLE',
                        "fields": []}
                ]}
        ]
        sa = []

        # print("showing each field")
        for bqi in schema2Dict:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)
        diff = DeepDiff(expectedResult, sa, ignore_order=True)
        self.assertEqual(diff, {},
                         "Unexpected result in toDict expected nothing insteadest got {}".format(
                             self.pp.pprint(diff)))

    def test_createschema(self):
        bqSchema = bqtools.create_schema(self.schemaTest1)
        expectedSchema = (
            bigquery.SchemaField('string', 'STRING'),
            bigquery.SchemaField('integer', 'INTEGER'),
            bigquery.SchemaField('float', 'FLOAT'),
            bigquery.SchemaField('boolean', 'BOOLEAN'),
            bigquery.SchemaField('record', 'RECORD', fields=(
                bigquery.SchemaField('string2', 'STRING'),
                bigquery.SchemaField('float', 'FLOAT'),
                bigquery.SchemaField('integer2', 'INTEGER'),
                bigquery.SchemaField('boolean2', 'BOOLEAN')
            )),
            bigquery.SchemaField('array', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('string3', 'STRING'),
                bigquery.SchemaField('integer3', 'INTEGER')
            ))
        )

        # print("testing result")
        # self.pp.pprint(bqSchema)
        sa = []

        # print("showing each field")
        for bqi in bqSchema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)
        # print("Schema as dict")
        # self.pp.pprint(sa)
        isa = sa

        # print("Expected result")
        # self.pp.pprint(expectedSchema)
        sa = []

        # print("showing each expected field")
        for bqi in expectedSchema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)
        # print("expected Schema as dict")
        diff = DeepDiff(isa, sa, ignore_order=True)
        # self.pp.pprint(diff)
        a = "Schema test1 schema does not match target {}".format(len(diff))
        self.assertEqual(diff, {}, a)

    def test_createschema2(self):
        # print("Creating a new schema")

        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        sa2 = []

        # print("showing each field schema2")
        for bqi in bqSchema2:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)
        # print("Schema2 as dict")
        # self.pp.pprint(sa2)
        expectedSchema2 = (
            bigquery.SchemaField('string', 'STRING'),
            bigquery.SchemaField('integer', 'INTEGER'),
            bigquery.SchemaField('record', 'RECORD', fields=(
                bigquery.SchemaField('string2', 'STRING'),
                bigquery.SchemaField('float', 'FLOAT'),
                bigquery.SchemaField('boolean2', 'BOOLEAN'),
                bigquery.SchemaField('appended1', 'STRING')
            )),
            bigquery.SchemaField('array', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('string3', 'STRING'),
                bigquery.SchemaField('integer3', 'INTEGER'),
                bigquery.SchemaField('foo', 'FLOAT')
            )),
            bigquery.SchemaField('anotherarray', 'RECORD', mode='REPEATED', fields=(
                bigquery.SchemaField('test1', 'INTEGER'),
                bigquery.SchemaField('test2', 'BOOLEAN')
            ))
        )
        sa = []
        for bqi in expectedSchema2:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)
        # self.pp.pprint(diff)
        a = "Schema test1 schema does not match target {}".format(diff)
        self.assertEqual(diff, {}, a)
        logger = logging.getLogger("testBQTools")

        evolved = bqtools.match_and_addtoschema({"string": "hello"}, expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 1")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52}, expectedSchema2,
                                                logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 2")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52, "record": {}},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 3")
        evolved = bqtools.match_and_addtoschema(
            {"string": "hello", "integer": 52, "record": {"string2": "hello2"}}, expectedSchema2,
            logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 4")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"}},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 6")

        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": []},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 7")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello"}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 8")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 9")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 10")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 11")
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False},
                                                                  {"test1": 52, "test2": True}]},
                                                expectedSchema2, logger=logger)
        self.assertEqual(evolved, False, "Expected no evolve but got evolve true evolve test 12")

        # evolve tests bbelow prepare baseline
        copyoforigschema = list(expectedSchema2)
        savedSchema = copy.deepcopy(copyoforigschema)

        sa = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa.append(i)

        # Evolutio test 1
        # add some stuff 2 layers down in an array
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False},
                                                                  {"test1": 52, "test2": True,
                                                                   "fred": "I am an evolved string",
                                                                   "iamanotherevolve": 32}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 13")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)
        diff = dict(diff)

        print(
            "============================================ evolve test 1 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 1 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolve',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected {}".format(self.pp.pformat(diff)))
        # Evolution test 2
        #  this just adds a fiedl at top level
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False},
                                                                  {"test1": 52, "test2": True}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 2 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 2 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}}}, diff,
                         "Schema evolution not as expected {}".format(self.pp.pformat(diff)))

        # Evolution test 3
        # this is an object with root schema evolution
        # Plus child objects with 2 different changes in them
        # plus another with both
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [{"test1": 52, "test2": False,
                                                                   "fred": "I am an evolution"},
                                                                  {"test1": 52, "test2": True,
                                                                   "iamanotherevolution": 1.3},
                                                                  {"test1": 52, "test2": True,
                                                                   "iamanotherevolution": 1.3,
                                                                   "fred": "I am same previous "
                                                                           "evolution"}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 3 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 3 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected {}".format(self.pp.pformat(diff)))
        # Evolution test 4
        # this is an object with root schema evolution
        # Plus child objects with 2 different changes in them
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [
                                                     {"test1": 52, "test2": False,
                                                      "fred": "I am an evolution"},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 4 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 4 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected")
        # Evolution test 5
        # add an array with strings an dno key this should fail
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [
                                                     {"test1": 52, "test2": False,
                                                      "fred": "I am an evolution"},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3,
                                                      "bill": ["hello", "fred", "break this"]}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 5 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 5 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'},
                                                                         {'description': None,
                                                                          'fields': [
                                                                              {'description': None,
                                                                               'fields': [],
                                                                               'mode': 'NULLABLE',
                                                                               'name': 'value',
                                                                               'type': 'STRING'}],
                                                                          'mode': 'REPEATED',
                                                                          'name': 'bill',
                                                                          'type': 'RECORD'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected")

        # Evolution test 6
        # add an array with strings an dno key this should fail
        copyoforigschema = copy.deepcopy(savedSchema)
        evolved = bqtools.match_and_addtoschema({"string": "hello", "integer": 52,
                                                 "hellomike": 3.1415926,
                                                 "record": {"string2": "hello2", "float": 1.3,
                                                            "boolean2": False,
                                                            "appended1": "another string"},
                                                 "array": [{"string3": "hello", "integer3": 42,
                                                            "foo": 3.141},
                                                           {"integer3": 42, "foo": 3.141}],
                                                 "anotherarray": [
                                                     {"test1": 52, "test2": False,
                                                      "fred": "I am an evolution"},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3},
                                                     {"test1": 52, "test2": True,
                                                      "iamanotherevolution": 1.3,
                                                      "bill": {}}]},
                                                copyoforigschema, logger=logger)
        self.assertEqual(evolved, True,
                         "Expected evolve but got no evolve False for evolve test 14")

        sa2 = []
        for bqi in copyoforigschema:
            i = bqtools.to_dict(bqi)
            # self.pp.pprint(i)
            sa2.append(i)

        diff = DeepDiff(sa, sa2, ignore_order=True)

        print(
            "============================================ evolve test 6 diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ evolve test 6 diff end  "
            "====================================")

        self.assertEqual({'iterable_item_added': {'root[4]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'fred',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name':
                                                                              'iamanotherevolution',
                                                                          'type': 'FLOAT'},
                                                                         {'description': None,
                                                                          'fields': [
                                                                              {'description': None,
                                                                               'fields': [],
                                                                               'mode': 'NULLABLE',
                                                                               'name':
                                                                                   'xxxDummySchemaAsNoneDefinedxxx',
                                                                               'type': 'STRING'}],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'bill',
                                                                          'type': 'RECORD'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [],
                                                              'mode': 'NULLABLE',
                                                              'name': 'hellomike',
                                                              'type': 'FLOAT'}},
                          'iterable_item_removed': {'root[4]': {'description': None,
                                                                'fields': [{'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test1',
                                                                            'type': 'INTEGER'},
                                                                           {'description': None,
                                                                            'fields': [],
                                                                            'mode': 'NULLABLE',
                                                                            'name': 'test2',
                                                                            'type': 'BOOLEAN'}],
                                                                'mode': 'REPEATED',
                                                                'name': 'anotherarray',
                                                                'type': 'RECORD'}}}, diff,
                         "Schema evolution not as expected")

    def test_patchbare(self):
        startschema = bqtools.create_schema(self.schema2startnobare)
        resultschema = bqtools.create_schema(self.schemaTest2nonbare)

        origobject = copy.deepcopy(self.schemaTest2bare)

        evolved = bqtools.match_and_addtoschema(self.schemaTest2bare, startschema)
        self.assertEqual(evolved, True,
                         "Bare llist and multi dict evolution has not happened as expected")
        diff = DeepDiff(resultschema, startschema, ignore_order=True)

        print(
            "============================================ mixed arrays added  diff start  "
            "====================================")
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), evolved))
        print(
            "============================================ mixed arrays added  diff end  "
            "====================================")

        bare_schema = bqtools.create_schema(origobject)
        views = bqtools.gen_diff_views('foo', 'ar', 'bob', bare_schema,
                                       description="A test schema")
        expected_views = [
            {"query":"""#standardSQL
SELECT
    _PARTITIONTIME AS scantime,
    xxrownumbering.partRowNumber,
    ifnull(tabob.string,"None") as `string`,
    ifnull(A1,"None") as stringarray,
    ifnull(tabob.record.appended1,"None") as `recordappended1`,
    ifnull(tabob.record.float,0.0) as `recordfloat`,
    ifnull(tabob.record.string2,"None") as `recordstring2`,
    ifnull(tabob.record.boolean2,False) as `recordboolean2`,
    ifnull(A2,0) as intarray,
    ifnull(tabob.integer,0) as `integer`,
    ifnull(A3.integer3,0) as `arrayinteger3`,
    ifnull(A3.foo,0.0) as `arrayfoo`,
    ifnull(A3.string3,"None") as `arraystring3`,
    ifnull(A4.a,"None") as `mixArraya`,
    ifnull(A4.b,"None") as `mixArrayb`,
    ifnull(A4.d,"None") as `mixArrayd`,
    ifnull(A4.c,0) as `mixArrayc`
from `foo.ar.bob` as tabob
LEFT JOIN UNNEST(tabob.stringarray) as A1
LEFT JOIN UNNEST(tabob.intarray) as A2
LEFT JOIN UNNEST(tabob.array) as A3
LEFT JOIN UNNEST(tabob.mixArray) as A4
    JOIN (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)) AS xxrownumbering
    ON
      _PARTITIONTIME = xxrownumbering.scantime
    """},
            {"query":"""#standardSQL
SELECT
  *
FROM (
  SELECT
    ifnull(earlier.scantime,
      later.scantime) AS scantime,
    CASE
      WHEN earlier.scantime IS NULL AND later.scantime IS NOT NULL THEN 1
      WHEN earlier.scantime IS NOT NULL
    AND later.scantime IS NULL THEN -1
    ELSE
    0
  END
    AS action,
    ARRAY((
      SELECT
        field
      FROM (
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "string"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "stringarray"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordappended1"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordfloat"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordstring2"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordboolean2"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "intarray"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "integer"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "arrayinteger3"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "arrayfoo"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "arraystring3"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "mixArraya"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "mixArrayb"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "mixArrayd"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "mixArrayc"
             ELSE CAST(null as string) END as field

            
            )
      WHERE
        field IS NOT NULL) ) AS updatedFields,
    ifnull(later.string,
      earlier.string) AS `string`,
ifnull(later.stringarray,
      earlier.stringarray) AS `stringarray`,
ifnull(later.recordappended1,
      earlier.recordappended1) AS `recordappended1`,
ifnull(later.recordfloat,
      earlier.recordfloat) AS `recordfloat`,
ifnull(later.recordstring2,
      earlier.recordstring2) AS `recordstring2`,
ifnull(later.recordboolean2,
      earlier.recordboolean2) AS `recordboolean2`,
ifnull(later.intarray,
      earlier.intarray) AS `intarray`,
ifnull(later.integer,
      earlier.integer) AS `integer`,
ifnull(later.arrayinteger3,
      earlier.arrayinteger3) AS `arrayinteger3`,
ifnull(later.arrayfoo,
      earlier.arrayfoo) AS `arrayfoo`,
ifnull(later.arraystring3,
      earlier.arraystring3) AS `arraystring3`,
ifnull(later.mixArraya,
      earlier.mixArraya) AS `mixArraya`,
ifnull(later.mixArrayb,
      earlier.mixArrayb) AS `mixArrayb`,
ifnull(later.mixArrayd,
      earlier.mixArrayd) AS `mixArrayd`,
ifnull(later.mixArrayc,
      earlier.mixArrayc) AS `mixArrayc`
  FROM 
     (#standardSQL
SELECT
    _PARTITIONTIME AS scantime,
    xxrownumbering.partRowNumber,
    ifnull(tabob.string,"None") as `string`,
    ifnull(A1,"None") as stringarray,
    ifnull(tabob.record.appended1,"None") as `recordappended1`,
    ifnull(tabob.record.float,0.0) as `recordfloat`,
    ifnull(tabob.record.string2,"None") as `recordstring2`,
    ifnull(tabob.record.boolean2,False) as `recordboolean2`,
    ifnull(A2,0) as intarray,
    ifnull(tabob.integer,0) as `integer`,
    ifnull(A3.integer3,0) as `arrayinteger3`,
    ifnull(A3.foo,0.0) as `arrayfoo`,
    ifnull(A3.string3,"None") as `arraystring3`,
    ifnull(A4.a,"None") as `mixArraya`,
    ifnull(A4.b,"None") as `mixArrayb`,
    ifnull(A4.d,"None") as `mixArrayd`,
    ifnull(A4.c,0) as `mixArrayc`
from `foo.ar.bob` as tabob
LEFT JOIN UNNEST(tabob.stringarray) as A1
LEFT JOIN UNNEST(tabob.intarray) as A2
LEFT JOIN UNNEST(tabob.array) as A3
LEFT JOIN UNNEST(tabob.mixArray) as A4
    JOIN (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)) AS xxrownumbering
    ON
      _PARTITIONTIME = xxrownumbering.scantime
    ) as later 
  FULL OUTER JOIN 
     (#standardSQL
SELECT
    _PARTITIONTIME AS scantime,
    xxrownumbering.partRowNumber,
    ifnull(tabob.string,"None") as `string`,
    ifnull(A1,"None") as stringarray,
    ifnull(tabob.record.appended1,"None") as `recordappended1`,
    ifnull(tabob.record.float,0.0) as `recordfloat`,
    ifnull(tabob.record.string2,"None") as `recordstring2`,
    ifnull(tabob.record.boolean2,False) as `recordboolean2`,
    ifnull(A2,0) as intarray,
    ifnull(tabob.integer,0) as `integer`,
    ifnull(A3.integer3,0) as `arrayinteger3`,
    ifnull(A3.foo,0.0) as `arrayfoo`,
    ifnull(A3.string3,"None") as `arraystring3`,
    ifnull(A4.a,"None") as `mixArraya`,
    ifnull(A4.b,"None") as `mixArrayb`,
    ifnull(A4.d,"None") as `mixArrayd`,
    ifnull(A4.c,0) as `mixArrayc`
from `foo.ar.bob` as tabob
LEFT JOIN UNNEST(tabob.stringarray) as A1
LEFT JOIN UNNEST(tabob.intarray) as A2
LEFT JOIN UNNEST(tabob.array) as A3
LEFT JOIN UNNEST(tabob.mixArray) as A4
    JOIN (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)) AS xxrownumbering
    ON
      _PARTITIONTIME = xxrownumbering.scantime
    
     -- avoid last row as full outer join this will attempt to find a row later
     -- that won't exist showing as a false delete
     WHERE 
    partRowNumber < (SELECT 
        MAX(partRowNumber)
    FROM (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)
    ))
) as earlier
  ON
    earlier.partRowNumber = later.partRowNumber -1
    AND earlier.string = later.string
AND earlier.stringarray = later.stringarray
AND earlier.recordappended1 = later.recordappended1
AND earlier.recordfloat = later.recordfloat
AND earlier.recordstring2 = later.recordstring2
AND earlier.recordboolean2 = later.recordboolean2
AND earlier.intarray = later.intarray
AND earlier.integer = later.integer
AND earlier.arrayinteger3 = later.arrayinteger3
AND earlier.arrayfoo = later.arrayfoo
AND earlier.arraystring3 = later.arraystring3
AND earlier.mixArraya = later.mixArraya
AND earlier.mixArrayb = later.mixArrayb
AND earlier.mixArrayd = later.mixArrayd
AND earlier.mixArrayc = later.mixArrayc
)
WHERE
  (action != 0 or array_length(updatedFields) > 0)
"""},
            {"query":"""#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.string IS NULL THEN 'Added'
    WHEN l.string IS NULL THEN 'Deleted'
    WHEN o.string = l.string AND o.stringarray = l.stringarray AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 AND o.intarray = l.intarray AND o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.mixArraya = l.mixArraya AND o.mixArrayb = l.mixArrayb AND o.mixArrayd = l.mixArrayd AND o.mixArrayc = l.mixArrayc THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.stringarray as origstringarray,
    l.stringarray as laterstringarray,
    case when o.stringarray = l.stringarray then 0 else 1 end as diffstringarray,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2,
    o.intarray as origintarray,
    l.intarray as laterintarray,
    case when o.intarray = l.intarray then 0 else 1 end as diffintarray,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.mixArraya as origmixArraya,
    l.mixArraya as latermixArraya,
    case when o.mixArraya = l.mixArraya then 0 else 1 end as diffmixArraya,
    o.mixArrayb as origmixArrayb,
    l.mixArrayb as latermixArrayb,
    case when o.mixArrayb = l.mixArrayb then 0 else 1 end as diffmixArrayb,
    o.mixArrayd as origmixArrayd,
    l.mixArrayd as latermixArrayd,
    case when o.mixArrayd = l.mixArrayd then 0 else 1 end as diffmixArrayd,
    o.mixArrayc as origmixArrayc,
    l.mixArrayc as latermixArrayc,
    case when o.mixArrayc = l.mixArrayc then 0 else 1 end as diffmixArrayc
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 1 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.string = o.string
    AND l.stringarray=o.stringarray
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2
    AND l.intarray=o.intarray
    AND l.integer=o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.mixArraya=o.mixArraya
    AND l.mixArrayb=o.mixArrayb
    AND l.mixArrayd=o.mixArrayd
    AND l.mixArrayc=o.mixArrayc"""},
            {"query":"""#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.string IS NULL THEN 'Added'
    WHEN l.string IS NULL THEN 'Deleted'
    WHEN o.string = l.string AND o.stringarray = l.stringarray AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 AND o.intarray = l.intarray AND o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.mixArraya = l.mixArraya AND o.mixArrayb = l.mixArrayb AND o.mixArrayd = l.mixArrayd AND o.mixArrayc = l.mixArrayc THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.stringarray as origstringarray,
    l.stringarray as laterstringarray,
    case when o.stringarray = l.stringarray then 0 else 1 end as diffstringarray,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2,
    o.intarray as origintarray,
    l.intarray as laterintarray,
    case when o.intarray = l.intarray then 0 else 1 end as diffintarray,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.mixArraya as origmixArraya,
    l.mixArraya as latermixArraya,
    case when o.mixArraya = l.mixArraya then 0 else 1 end as diffmixArraya,
    o.mixArrayb as origmixArrayb,
    l.mixArrayb as latermixArrayb,
    case when o.mixArrayb = l.mixArrayb then 0 else 1 end as diffmixArrayb,
    o.mixArrayd as origmixArrayd,
    l.mixArrayd as latermixArrayd,
    case when o.mixArrayd = l.mixArrayd then 0 else 1 end as diffmixArrayd,
    o.mixArrayc as origmixArrayc,
    l.mixArrayc as latermixArrayc,
    case when o.mixArrayc = l.mixArrayc then 0 else 1 end as diffmixArrayc
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 7 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.string = o.string
    AND l.stringarray=o.stringarray
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2
    AND l.intarray=o.intarray
    AND l.integer=o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.mixArraya=o.mixArraya
    AND l.mixArrayb=o.mixArrayb
    AND l.mixArrayd=o.mixArrayd
    AND l.mixArrayc=o.mixArrayc"""},
            {"query":"""#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.string IS NULL THEN 'Added'
    WHEN l.string IS NULL THEN 'Deleted'
    WHEN o.string = l.string AND o.stringarray = l.stringarray AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 AND o.intarray = l.intarray AND o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.mixArraya = l.mixArraya AND o.mixArrayb = l.mixArrayb AND o.mixArrayd = l.mixArrayd AND o.mixArrayc = l.mixArrayc THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.stringarray as origstringarray,
    l.stringarray as laterstringarray,
    case when o.stringarray = l.stringarray then 0 else 1 end as diffstringarray,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2,
    o.intarray as origintarray,
    l.intarray as laterintarray,
    case when o.intarray = l.intarray then 0 else 1 end as diffintarray,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.mixArraya as origmixArraya,
    l.mixArraya as latermixArraya,
    case when o.mixArraya = l.mixArraya then 0 else 1 end as diffmixArraya,
    o.mixArrayb as origmixArrayb,
    l.mixArrayb as latermixArrayb,
    case when o.mixArrayb = l.mixArrayb then 0 else 1 end as diffmixArrayb,
    o.mixArrayd as origmixArrayd,
    l.mixArrayd as latermixArrayd,
    case when o.mixArrayd = l.mixArrayd then 0 else 1 end as diffmixArrayd,
    o.mixArrayc as origmixArrayc,
    l.mixArrayc as latermixArrayc,
    case when o.mixArrayc = l.mixArrayc then 0 else 1 end as diffmixArrayc
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 30 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.string = o.string
    AND l.stringarray=o.stringarray
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2
    AND l.intarray=o.intarray
    AND l.integer=o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.mixArraya=o.mixArraya
    AND l.mixArrayb=o.mixArrayb
    AND l.mixArrayd=o.mixArrayd
    AND l.mixArrayc=o.mixArrayc"""},
            {"query":"""#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.string IS NULL THEN 'Added'
    WHEN l.string IS NULL THEN 'Deleted'
    WHEN o.string = l.string AND o.stringarray = l.stringarray AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 AND o.intarray = l.intarray AND o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.mixArraya = l.mixArraya AND o.mixArrayb = l.mixArrayb AND o.mixArrayd = l.mixArrayd AND o.mixArrayc = l.mixArrayc THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.stringarray as origstringarray,
    l.stringarray as laterstringarray,
    case when o.stringarray = l.stringarray then 0 else 1 end as diffstringarray,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2,
    o.intarray as origintarray,
    l.intarray as laterintarray,
    case when o.intarray = l.intarray then 0 else 1 end as diffintarray,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.mixArraya as origmixArraya,
    l.mixArraya as latermixArraya,
    case when o.mixArraya = l.mixArraya then 0 else 1 end as diffmixArraya,
    o.mixArrayb as origmixArrayb,
    l.mixArrayb as latermixArrayb,
    case when o.mixArrayb = l.mixArrayb then 0 else 1 end as diffmixArrayb,
    o.mixArrayd as origmixArrayd,
    l.mixArrayd as latermixArrayd,
    case when o.mixArrayd = l.mixArrayd then 0 else 1 end as diffmixArrayd,
    o.mixArrayc as origmixArrayc,
    l.mixArrayc as latermixArrayc,
    case when o.mixArrayc = l.mixArrayc then 0 else 1 end as diffmixArrayc
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 14 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.string = o.string
    AND l.stringarray=o.stringarray
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2
    AND l.intarray=o.intarray
    AND l.integer=o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.mixArraya=o.mixArraya
    AND l.mixArrayb=o.mixArrayb
    AND l.mixArrayd=o.mixArrayd
    AND l.mixArrayc=o.mixArrayc"""}
        ]
        for i,vi in enumerate(views):
            self.assertEqual(vi["query"],expected_views[i]["query"],"Bare list diff  {}".format(i))

    def test_patch(self):

        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        bqSchema = bqtools.create_schema(self.schemaTest1)

        sa = []

        for bqi in bqSchema:
            i = bqtools.to_dict(bqi)
            sa.append(i)

        osa = copy.deepcopy(sa)

        change, pschema = bqtools.recurse_and_add_to_schema(bqSchema2, sa)
        diff = DeepDiff(pschema, osa, ignore_order=True)

        # patching never removes fields so expect additions
        # so after list of root[] should be one longer
        expectedDiff = {'iterable_item_added': {'root[2]': {'description': None,
                                                            'fields': [{'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'integer2',
                                                                        'type': 'INTEGER'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'float',
                                                                        'type': 'FLOAT'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'string2',
                                                                        'type': 'STRING'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'boolean2',
                                                                        'type': 'BOOLEAN'}],
                                                            'mode': 'NULLABLE',
                                                            'name': 'record',
                                                            'type': 'RECORD'},
                                                'root[5]': {'description': None,
                                                            'fields': [{'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'integer3',
                                                                        'type': 'INTEGER'},
                                                                       {'description': None,
                                                                        'fields': [],
                                                                        'mode': 'NULLABLE',
                                                                        'name': 'string3',
                                                                        'type': 'STRING'}],
                                                            'mode': 'REPEATED',
                                                            'name': 'array',
                                                            'type': 'RECORD'}},
                        'iterable_item_removed': {'root[2]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'integer2',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'float',
                                                                          'type': 'FLOAT'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'string2',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'boolean2',
                                                                          'type': 'BOOLEAN'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'appended1',
                                                                          'type': 'STRING'}],
                                                              'mode': 'NULLABLE',
                                                              'name': 'record',
                                                              'type': 'RECORD'},
                                                  'root[5]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'integer3',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'string3',
                                                                          'type': 'STRING'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'foo',
                                                                          'type': 'FLOAT'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'array',
                                                              'type': 'RECORD'},
                                                  'root[6]': {'description': None,
                                                              'fields': [{'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test1',
                                                                          'type': 'INTEGER'},
                                                                         {'description': None,
                                                                          'fields': [],
                                                                          'mode': 'NULLABLE',
                                                                          'name': 'test2',
                                                                          'type': 'BOOLEAN'}],
                                                              'mode': 'REPEATED',
                                                              'name': 'anotherarray',
                                                              'type': 'RECORD'}}}

        self.assertEqual(diff, expectedDiff,
                         "Patch diff is not what is expected {}".format(self.pp.pformat(diff)))
        self.assertEqual(change, True,
                         "Patch diff change result {} is not what is expected {}".format(change,
                                                                                         self.pp.pformat(
                                                                                             diff)))

        bqSchema3 = bqtools.create_schema(self.schemaTest3)
        bqSchema4 = bqtools.create_schema(self.schemaTest4)

        sa2 = []

        for bqi in bqSchema3:
            i = bqtools.to_dict(bqi)
            sa2.append(i)

        osa = copy.deepcopy(sa2)

        change, pschema = bqtools.recurse_and_add_to_schema(bqSchema4, sa2)
        diff = DeepDiff(pschema, osa, ignore_order=True)
        print("Patched schema diff {} change{}".format(self.pp.pformat(diff), change))

    #        print("old {}".format(self.pp.pformat(osa)))
    #        print("new {}".format(self.pp.pformat(pschema)))

    def test_compile(self):
        dataset = MockDataset("a","b")
        datasetav = MockDataset("a","c")
        datasetin = MockDataset("a","a")
        datasetav2 = MockDataset("b","c")

        """
        graphviz  http://www.webgraphviz.com/
        to visualize what this does
        digraph G {
          "a.b.z"
          "a.b.x"
          "a.c.x" -> "a.b.x"
          "a.c.x2"-> "a.b.z"
          "a.c.x2"-> "a.b.x"
          "b.c.x3" -> "a.b.z" 
          "b.c.x3" -> "a.c.x2" 
          "a.c.x4" -> "a.b.z"
          "a.c.x4" -> "a.c.x"
          "a.c.x4" -> "b.c.x3"
        }
        """


        compiler = bqtools.ViewCompiler()
        # tranche 0 a.b.z
        compiler.add_view_to_process(dataset,"z","""#standardSQL
select 1 
from `a.a.input1`""")
        # tranche 0 a.b.x
        compiler.add_view_to_process(dataset, "x", """#standardSQL
select 1 
from `a.a.input2`
where 1=0""")
        # tranche 1 a.c.x
        compiler.add_view_to_process(datasetav, "x", """#standardSQL
select 1 
from `a.b.x`
where 1=0""")
        # tranche 1 a.c.x2
        compiler.add_view_to_process(datasetav, "x2", """#standardSQL
select 1 
from `a.b.z`
join `a.b.x`
where 1=0""")
        # tranche 2 b.c.x3
        compiler.add_view_to_process(datasetav2, "x3", """#standardSQL
select 1 
from `a.b.z`
join `a.c.x2`
where 1=0""")
        # tranches 3 a.c.x4
        compiler.add_view_to_process(datasetav, "x4", """#standardSQL
select 1 
from `a.b.z`
join `a.c.x`
join `b.c.x3`
where 1=0""")
        tranches = []
        for view_tranche in compiler.view_tranche:
            tranches.append(view_tranche)

        self.assertEqual(len(tranches),4,"Unexpcted number of uncompiled view tranches")
        self.assertEqual(len(tranches[0]),2,"Unexpected number of tranche 0 views")
        self.assertEqual("a.b.z" in tranches[0], True, "Unexpected view a.b.z missing from tranche 0")
        self.assertEqual("a.b.x" in tranches[0], True, "Unexpected view a.b.x missing from tranche 0")
        self.assertEqual(len(tranches[1]), 2, "Unexpected number of tranche 1 views")
        self.assertEqual("a.c.x" in tranches[1], True, "Unexpected view a.c.x missing from tranche 1")
        self.assertEqual("a.c.x2" in tranches[1], True, "Unexpected view a.c.x2 missing from tranche 1")
        self.assertEqual(len(tranches[2]), 1, "Unexpected number of tranche 2 views")
        self.assertEqual("b.c.x3" in tranches[2], True,
                         "Unexpected view b.c.x3missing from tranche 2")
        self.assertEqual(len(tranches[3]), 1, "Unexpected number of tranche 2 views")
        self.assertEqual("a.c.x4" in tranches[3], True,
                         "Unexpected view a.c.x4 missing from tranche 2")

        compiler.compile_views()

        for view_tranche in compiler.view_tranche:
            for view in compiler.view_in_tranche(view_tranche):
                pass


    def test_patch2(self):

        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        bqSchema = bqtools.create_schema(self.schemaTest2)

        sa = []

        for bqi in bqSchema:
            i = bqtools.to_dict(bqi)
            sa.append(i)

        osa = copy.deepcopy(sa)

        change, pschema = bqtools.recurse_and_add_to_schema(bqSchema2, sa)
        diff = DeepDiff(pschema, osa, ignore_order=True)

        # patching never removes fields so expect additions
        # so after list of root[] should be one longer
        expectedDiff = {}

        self.assertEqual(diff, expectedDiff,
                         "Patch diff is not what is expected {}".format(self.pp.pformat(diff)))

        self.assertEqual(change, False,
                         "Patch diff change result {} is not what is expected {}".format(change,
                                                                                         self.pp.pformat(
                                                                                             diff)))

    #        print("Patched schema diff {}".format(self.pp.pformat(diff)))
    #        print("old {}".format(self.pp.pformat(osa)))
    #        print("new {}".format(self.pp.pformat(pschema)))

    def test_sync(self):

        logging.basicConfig(level=logging.INFO)

        # get target datasets ready uses app default credentials
        bqclient = bigquery.Client()
        stclient = storage.Client()

        # will use default project and public datsets for testing
        destination_project = bqclient.project

        # going to copy data from various datasets in bigquery-public-data project
        # each destination will be of the form bqsynctest_<region>_<orignaldatasetname>
        # in region - will be replaced with _ to make valid dataset nae
        # as all public data is in us we will need for cross region a us bucket
        # and a target region bucket
        # tests are in region i.e. us to us
        # us to eu
        # us to europe-west2
        # bucket names will be created if they do not exist of
        # bqsynctest_<projectid>_<region>
        # eac  bucket will have a 1 day lifecycle added
        # source will be picked with various source attribute types, partitioning and clustering strategy
        # success is tables are copied no errors in extract, load or copy
        # not tale numbers may vary
        # at end the test datasets will be deleted the buckets will remain
        # this as bucket names remain reserved for sometime after deletion
        test_buckets = []

        usbucket = "bqsynctest_{}_us".format(destination_project)
        test_buckets.append({"name":usbucket,"region":"us"})
        eubucket = "bqsynctest_{}_eu".format(destination_project)
        test_buckets.append({"name":eubucket,"region":"eu"})
        eu2bucket = "bqsynctest_{}_europe-west-2".format(destination_project)
        test_buckets.append({"name":eu2bucket,"region":"europe-west2"})

        logging.info("Checking buckets for bqsync tests exist in right regions and with lifecycle rules...")

        # loop through test bucket if they do not exist create in the right region and add
        # #lifecycle rule
        # if they do exist check they are in right region and have the expected lifecycle rule
        for bucket_dict in test_buckets:
            bucket = None
            try:
                bucket = stclient.get_bucket(bucket_dict["name"])
            except exceptions.NotFound:
                bucket_ref = storage.Bucket(stclient,name=bucket_dict["name"])
                bucket_ref.location = bucket_dict["region"]
                storage.Bucket.create(bucket_ref,stclient)
                bucket = stclient.get_bucket(bucket_dict["name"])
            rules = bucket.lifecycle_rules
            nrules = []
            found1daydeletrule = False
            for rule in rules:
                if isinstance(rule, dict):
                    if "condition" in rule and "age" in rule["condition"] and rule["condition"][
                        "age"] == 1 and "isLive" in rule["condition"] and rule["condition"][
                        "isLive"]:
                        found1daydeletrule = True
                nrules.append(rule)
            if not found1daydeletrule:
                nrules.append(
                    {"action": {"type": "Delete"}, "condition": {"age": 1, "isLive": True}})
            bucket.lifecycle_rules = nrules
            bucket.update(stclient)

        # starting datsets to test with form project bigquery-public-data
        # along with each entry is list of tables and length of maximum days for day partition
        test_source_configs = []

        # small dataset good to start tests basic types
        test_source_configs.append({
            "description":"small dataset good to start tests basic types",
            "dataset_name":"fcc_political_ads",
            "table_filter_regexp":['broadcast_tv_radio_station',
                                   'content_info',
                                   'file_history',
                                   'file_record'],
            "max_last_days":365
        })
        # small dataset good to start tests basic types
        test_source_configs.append({
            "description": "date partitioned 1 date type field",
            "dataset_name": "wikipedia",
            "table_filter_regexp": ['wikidata'],
            "max_last_days": None
        })
        # a table with geography data type
        test_source_configs.append({
            "description":"a table with geography data type",
            "dataset_name": "faa",
            "table_filter_regexp": ['us_airports'],
            "max_last_days": 365
        })
        # a dataset with a day partitioned table with  clustering
        # not using a specific partition column name so  just ingest time
        test_source_configs.append({
            "description":"a dataset with a day partitioned table with  clustering not using a specific partition column name so  just ingest time",
            "dataset_name": "new_york_subway",
            "table_filter_regexp": ['geo_nyc_borough_boundaries'],
            "max_last_days": 365
        })
        # a dataset with view referencing it self to demo simple view copying
        test_source_configs.append({
            "description":"a dataset with view referencing it self to demo simple view copying",
            "dataset_name": "noaa_goes16",
            "table_filter_regexp": ['.*'],
            "max_last_days": 365
        })
        # a dataset with functions only
        test_source_configs.append({
            "description":"a dataset with functions only",
            "dataset_name": "persistent_udfs",
            "table_filter_regexp": ['.*'],
            "max_last_days": 365
        })
        # a dataset with nested table example and a model
        # models will fail
        test_source_configs.append({
            "description":"a dataset with nested table example and a model",
            "dataset_name": "samples",
            "table_filter_regexp": ['github_nested','model'],
            "max_last_days": 365
        })
        # a dataset with day partioned no clustering using natural load time
        test_source_configs.append({
            "description":"a dataset with day partioned no clustering using natural load time",
            "dataset_name": "sec_quarterly_financials",
            "table_filter_regexp": ['.*'],
            "max_last_days": 365 * 3
        })
        # a dataset with a day partitioned table with clustering
        # using a specific partition column name so not just ingest time
        # has repetade basic types
        # note this shows the issue of bq nit correctly supporting avro logical types
        # https://issuetracker.google.com/issues/35905894 will fail until resolved
        test_source_configs.append({
            "description":"a dataset with a day partitioned table with clustering using a specific partition column name so not just ingest time",
            "dataset_name": "human_genome_variants",
            "table_filter_regexp": ['platinum_genomes_deepvariant_variants_20180823'],
            "max_last_days": None
        })
        test_source_configs = []
        test_destination_datasets_list = []
        for src_destination in test_source_configs:
            tests = []
            # set up local us test
            destdatset = "bqsynctest_{}_{}".format("US",src_destination["dataset_name"]).replace("-","_")
            tests.append({
                "subtest":"us intra region",
                "destdataset": destdatset,
                "destregion":"US"
            })
            test_destination_datasets_list.append(destdatset)
            # set up us to eu test
            destdatset = "bqsynctest_{}_{}".format("EU", src_destination["dataset_name"]).replace(
                "-", "_")
            tests.append({
                "subtest": "us to eu cross region",
                "destdataset": destdatset,
                "destregion": "EU",
                "dstbucket":eubucket
            })
            test_destination_datasets_list.append(destdatset)
            # set up us to europe-west2 test
            # set up us to eu test
            destdatset = "bqsynctest_{}_{}".format("europe-west2", src_destination["dataset_name"]).replace(
                "-", "_")
            tests.append({
                "subtest": "us to eu cross region",
                "destdataset": destdatset,
                "destregion": "europe-west2",
                "dstbucket":eu2bucket
            })
            test_destination_datasets_list.append(destdatset)
            src_destination["tests"] = tests

        logging.info(
            "Checking daatsets for bqsync tests exist in right regions and if exist empty them i.e. delete and recreate them...")
        for datasetname in test_destination_datasets_list:
            dataset_ref = bqclient.dataset(datasetname)
            if bqtools.dataset_exists(bqclient,dataset_ref):
                bqclient.delete_dataset(bqclient.get_dataset(dataset_ref),delete_contents=True)

        # for each source run sub tests
        logging.info("Staring tests...")
        # uncomment below if sync tests not required
        # test_source_configs =[]
        for test_config in test_source_configs:

            # run sub test basically an initial copy followed by
            # 2nd copy if no data latter should do nothing
            for dstconfig in test_config["tests"]:

                # create an empty dataset
                dataset_ref = bqclient.dataset(dstconfig["destdataset"])
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = dstconfig["destregion"]
                dataset = bqclient.create_dataset(dataset)

                # create initial sync
                # as source is all in US if not us must need buckets
                synctest = None
                if dstconfig["destregion"] == "US":
                    synctest = bqtools.MultiBQSyncCoordinator(["bigquery-public-data.{}".format(test_config["dataset_name"])],
                                                   ["{}.{}".format(destination_project,dstconfig["destdataset"])],
                                                   remove_deleted_tables=True,
                                                   copy_data=True,
                                                   copy_types=["TABLE","VIEW","ROUTINE","MODEL"],
                                                   check_depth=0,
                                                   table_view_filter=test_config["table_filter_regexp"],
                                                   table_or_views_to_exclude=[],
                                                   latest_date=None,
                                                   days_before_latest_day=test_config["max_last_days"],
                                                   day_partition_deep_check=False,
                                                   analysis_project=destination_project)
                else:
                    synctest = bqtools.MultiBQSyncCoordinator(
                            ["bigquery-public-data.{}".format(test_config["dataset_name"])],
                            ["{}.{}".format(destination_project, dstconfig["destdataset"])],
                            srcbucket=usbucket,
                            dstbucket=dstconfig["dstbucket"],
                            remove_deleted_tables=True,
                            copy_data=True,
                            copy_types=["TABLE","VIEW","ROUTINE","MODEL"],
                            check_depth=0,
                            table_view_filter=test_config["table_filter_regexp"],
                            table_or_views_to_exclude=[],
                            latest_date=None,
                            days_before_latest_day=test_config["max_last_days"],
                            day_partition_deep_check=False,
                            analysis_project=destination_project)
                synctest.sync()
                self.assertEqual(True, True, "Initial Sync {} {} from bigquery-public-data..{} with {}.{}  completed".format(
                    test_config["description"],
                    dstconfig["subtest"],
                    test_config["dataset_name"],
                    destination_project,
                    dstconfig["destdataset"]
                ))
                synctest.reset_stats()
                synctest.sync()
                self.assertEqual(synctest.tables_avoided, synctest.tables_synced,
                                 "Second Sync {} {} from bigquery-public-data..{} with {}.{}  "
                                 "completed".format(
                                     test_config["description"],
                                     dstconfig["subtest"],
                                     test_config["dataset_name"],
                                     destination_project,
                                     dstconfig["destdataset"]
                                 ))
            eutest = bqtools.MultiBQSyncCoordinator(
                ["{}.{}".format(destination_project,test_config["tests"][1]["destdataset"])],
                ["{}.{}".format(destination_project,test_config["tests"][2]["destdataset"])],
                srcbucket=eubucket,
                dstbucket=eu2bucket,
                remove_deleted_tables=True,
                copy_data=True,
                copy_types=["TABLE", "VIEW", "ROUTINE", "MODEL"],
                check_depth=0,
                table_view_filter=[".*"],
                table_or_views_to_exclude=[],
                latest_date=None,
                days_before_latest_day=None,
                day_partition_deep_check=False,
                analysis_project=destination_project)
            eutest.sync()
            self.assertEqual(eutest.tables_avoided + eutest.view_avoided + eutest.routines_avoided,
                             eutest.tables_synced + eutest.views_synced + eutest.routines_synced,
                             "Inter europe Sync {} {} from {}.{} with {}.{}"
                             "completed".format(
                                 test_config["description"],
                                 "EU to europe-west2",
                                 destination_project,
                                 test_config["tests"][1]["destdataset"],
                                 destination_project,
                                 test_config["tests"][2]["destdataset"]
                             ))

    def test_gendiff(self):
        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        views = bqtools.gen_diff_views('foo', 'ar', 'bob', bqSchema2, description="A test schema")

        vexpected = {'bobdb': {
            "query": """#standardSQL
SELECT
    _PARTITIONTIME AS scantime,
    xxrownumbering.partRowNumber,
    ifnull(tabob.integer,0) as `integer`,
    ifnull(A1.integer3,0) as `arrayinteger3`,
    ifnull(A1.foo,0.0) as `arrayfoo`,
    ifnull(A1.string3,"None") as `arraystring3`,
    ifnull(A2.test1,0) as `anotherarraytest1`,
    ifnull(A2.test2,False) as `anotherarraytest2`,
    ifnull(tabob.string,"None") as `string`,
    ifnull(tabob.record.appended1,"None") as `recordappended1`,
    ifnull(tabob.record.float,0.0) as `recordfloat`,
    ifnull(tabob.record.string2,"None") as `recordstring2`,
    ifnull(tabob.record.boolean2,False) as `recordboolean2`
from `foo.ar.bob` as tabob
LEFT JOIN UNNEST(tabob.array) as A1
LEFT JOIN UNNEST(tabob.anotherarray) as A2
    JOIN (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)) AS xxrownumbering
    ON
      _PARTITIONTIME = xxrownumbering.scantime
    """,
            "description": "View used as basis for diffview:A test schema"},
                     'bobdiff' : {
                         "query":"""#standardSQL
SELECT
  *
FROM (
  SELECT
    ifnull(earlier.scantime,
      later.scantime) AS scantime,
    CASE
      WHEN earlier.scantime IS NULL AND later.scantime IS NOT NULL THEN 1
      WHEN earlier.scantime IS NOT NULL
    AND later.scantime IS NULL THEN -1
    ELSE
    0
  END
    AS action,
    ARRAY((
      SELECT
        field
      FROM (
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "integer"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "arrayinteger3"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "arrayfoo"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "arraystring3"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "anotherarraytest1"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "anotherarraytest2"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "string"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordappended1"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordfloat"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordstring2"
             ELSE CAST(null as string) END as field

     UNION ALL
         SELECT 
           CASE 
              WHEN earlier.scantime IS NULL or later.scantime IS NULL then "recordboolean2"
             ELSE CAST(null as string) END as field

            
            )
      WHERE
        field IS NOT NULL) ) AS updatedFields,
    ifnull(later.integer,
      earlier.integer) AS `integer`,
ifnull(later.arrayinteger3,
      earlier.arrayinteger3) AS `arrayinteger3`,
ifnull(later.arrayfoo,
      earlier.arrayfoo) AS `arrayfoo`,
ifnull(later.arraystring3,
      earlier.arraystring3) AS `arraystring3`,
ifnull(later.anotherarraytest1,
      earlier.anotherarraytest1) AS `anotherarraytest1`,
ifnull(later.anotherarraytest2,
      earlier.anotherarraytest2) AS `anotherarraytest2`,
ifnull(later.string,
      earlier.string) AS `string`,
ifnull(later.recordappended1,
      earlier.recordappended1) AS `recordappended1`,
ifnull(later.recordfloat,
      earlier.recordfloat) AS `recordfloat`,
ifnull(later.recordstring2,
      earlier.recordstring2) AS `recordstring2`,
ifnull(later.recordboolean2,
      earlier.recordboolean2) AS `recordboolean2`
  FROM 
     (#standardSQL
SELECT
    _PARTITIONTIME AS scantime,
    xxrownumbering.partRowNumber,
    ifnull(tabob.integer,0) as `integer`,
    ifnull(A1.integer3,0) as `arrayinteger3`,
    ifnull(A1.foo,0.0) as `arrayfoo`,
    ifnull(A1.string3,"None") as `arraystring3`,
    ifnull(A2.test1,0) as `anotherarraytest1`,
    ifnull(A2.test2,False) as `anotherarraytest2`,
    ifnull(tabob.string,"None") as `string`,
    ifnull(tabob.record.appended1,"None") as `recordappended1`,
    ifnull(tabob.record.float,0.0) as `recordfloat`,
    ifnull(tabob.record.string2,"None") as `recordstring2`,
    ifnull(tabob.record.boolean2,False) as `recordboolean2`
from `foo.ar.bob` as tabob
LEFT JOIN UNNEST(tabob.array) as A1
LEFT JOIN UNNEST(tabob.anotherarray) as A2
    JOIN (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)) AS xxrownumbering
    ON
      _PARTITIONTIME = xxrownumbering.scantime
    ) as later 
  FULL OUTER JOIN 
     (#standardSQL
SELECT
    _PARTITIONTIME AS scantime,
    xxrownumbering.partRowNumber,
    ifnull(tabob.integer,0) as `integer`,
    ifnull(A1.integer3,0) as `arrayinteger3`,
    ifnull(A1.foo,0.0) as `arrayfoo`,
    ifnull(A1.string3,"None") as `arraystring3`,
    ifnull(A2.test1,0) as `anotherarraytest1`,
    ifnull(A2.test2,False) as `anotherarraytest2`,
    ifnull(tabob.string,"None") as `string`,
    ifnull(tabob.record.appended1,"None") as `recordappended1`,
    ifnull(tabob.record.float,0.0) as `recordfloat`,
    ifnull(tabob.record.string2,"None") as `recordstring2`,
    ifnull(tabob.record.boolean2,False) as `recordboolean2`
from `foo.ar.bob` as tabob
LEFT JOIN UNNEST(tabob.array) as A1
LEFT JOIN UNNEST(tabob.anotherarray) as A2
    JOIN (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)) AS xxrownumbering
    ON
      _PARTITIONTIME = xxrownumbering.scantime
    
     -- avoid last row as full outer join this will attempt to find a row later
     -- that won't exist showing as a false delete
     WHERE 
    partRowNumber < (SELECT 
        MAX(partRowNumber)
    FROM (
      SELECT
        scantime,
        ROW_NUMBER() OVER(ORDER BY scantime) AS partRowNumber
      FROM (
        SELECT
          DISTINCT _PARTITIONTIME AS scantime,
        FROM
          `foo.ar.bob`)
    ))
) as earlier
  ON
    earlier.partRowNumber = later.partRowNumber -1
    AND earlier.integer = later.integer
AND earlier.arrayinteger3 = later.arrayinteger3
AND earlier.arrayfoo = later.arrayfoo
AND earlier.arraystring3 = later.arraystring3
AND earlier.anotherarraytest1 = later.anotherarraytest1
AND earlier.anotherarraytest2 = later.anotherarraytest2
AND earlier.string = later.string
AND earlier.recordappended1 = later.recordappended1
AND earlier.recordfloat = later.recordfloat
AND earlier.recordstring2 = later.recordstring2
AND earlier.recordboolean2 = later.recordboolean2
)
WHERE
  (action != 0 or array_length(updatedFields) > 0)
""",
                         "description":'View calculates what has changed at what time:A test schema'
                     },
                     'bobdiffday': {
                         "query": """#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.integer IS NULL THEN 'Added'
    WHEN l.integer IS NULL THEN 'Deleted'
    WHEN o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.anotherarraytest1 = l.anotherarraytest1 AND o.anotherarraytest2 = l.anotherarraytest2 AND o.string = l.string AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.anotherarraytest1 as origanotherarraytest1,
    l.anotherarraytest1 as lateranotherarraytest1,
    case when o.anotherarraytest1 = l.anotherarraytest1 then 0 else 1 end as diffanotherarraytest1,
    o.anotherarraytest2 as origanotherarraytest2,
    l.anotherarraytest2 as lateranotherarraytest2,
    case when o.anotherarraytest2 = l.anotherarraytest2 then 0 else 1 end as diffanotherarraytest2,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 1 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.integer = o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.anotherarraytest1=o.anotherarraytest1
    AND l.anotherarraytest2=o.anotherarraytest2
    AND l.string=o.string
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2""",
                         "description": "Diff of day of underlying table bob description: A test schema"},
                     'bobdiffweek': {'query': """#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.integer IS NULL THEN 'Added'
    WHEN l.integer IS NULL THEN 'Deleted'
    WHEN o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.anotherarraytest1 = l.anotherarraytest1 AND o.anotherarraytest2 = l.anotherarraytest2 AND o.string = l.string AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.anotherarraytest1 as origanotherarraytest1,
    l.anotherarraytest1 as lateranotherarraytest1,
    case when o.anotherarraytest1 = l.anotherarraytest1 then 0 else 1 end as diffanotherarraytest1,
    o.anotherarraytest2 as origanotherarraytest2,
    l.anotherarraytest2 as lateranotherarraytest2,
    case when o.anotherarraytest2 = l.anotherarraytest2 then 0 else 1 end as diffanotherarraytest2,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 7 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.integer = o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.anotherarraytest1=o.anotherarraytest1
    AND l.anotherarraytest2=o.anotherarraytest2
    AND l.string=o.string
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2""",
                                     'description': 'Diff of week of underlying table bob description: A '
                                                    'test schema'},
                     'bobdiffmonth': {'query': """#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.integer IS NULL THEN 'Added'
    WHEN l.integer IS NULL THEN 'Deleted'
    WHEN o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.anotherarraytest1 = l.anotherarraytest1 AND o.anotherarraytest2 = l.anotherarraytest2 AND o.string = l.string AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.anotherarraytest1 as origanotherarraytest1,
    l.anotherarraytest1 as lateranotherarraytest1,
    case when o.anotherarraytest1 = l.anotherarraytest1 then 0 else 1 end as diffanotherarraytest1,
    o.anotherarraytest2 as origanotherarraytest2,
    l.anotherarraytest2 as lateranotherarraytest2,
    case when o.anotherarraytest2 = l.anotherarraytest2 then 0 else 1 end as diffanotherarraytest2,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 30 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.integer = o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.anotherarraytest1=o.anotherarraytest1
    AND l.anotherarraytest2=o.anotherarraytest2
    AND l.string=o.string
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2""",
                                      'description': 'Diff of month of underlying table bob description: A '
                                                     'test schema'},
                     'bobdifffortnight': {'query': """#standardSQL
SELECT
    o.scantime as origscantime,
    l.scantime as laterscantime,
    CASE
    WHEN o.integer IS NULL THEN 'Added'
    WHEN l.integer IS NULL THEN 'Deleted'
    WHEN o.integer = l.integer AND o.arrayinteger3 = l.arrayinteger3 AND o.arrayfoo = l.arrayfoo AND o.arraystring3 = l.arraystring3 AND o.anotherarraytest1 = l.anotherarraytest1 AND o.anotherarraytest2 = l.anotherarraytest2 AND o.string = l.string AND o.recordappended1 = l.recordappended1 AND o.recordfloat = l.recordfloat AND o.recordstring2 = l.recordstring2 AND o.recordboolean2 = l.recordboolean2 THEN 'Same'
    ELSE 'Updated'
  END AS action,
    o.integer as originteger,
    l.integer as laterinteger,
    case when o.integer = l.integer then 0 else 1 end as diffinteger,
    o.arrayinteger3 as origarrayinteger3,
    l.arrayinteger3 as laterarrayinteger3,
    case when o.arrayinteger3 = l.arrayinteger3 then 0 else 1 end as diffarrayinteger3,
    o.arrayfoo as origarrayfoo,
    l.arrayfoo as laterarrayfoo,
    case when o.arrayfoo = l.arrayfoo then 0 else 1 end as diffarrayfoo,
    o.arraystring3 as origarraystring3,
    l.arraystring3 as laterarraystring3,
    case when o.arraystring3 = l.arraystring3 then 0 else 1 end as diffarraystring3,
    o.anotherarraytest1 as origanotherarraytest1,
    l.anotherarraytest1 as lateranotherarraytest1,
    case when o.anotherarraytest1 = l.anotherarraytest1 then 0 else 1 end as diffanotherarraytest1,
    o.anotherarraytest2 as origanotherarraytest2,
    l.anotherarraytest2 as lateranotherarraytest2,
    case when o.anotherarraytest2 = l.anotherarraytest2 then 0 else 1 end as diffanotherarraytest2,
    o.string as origstring,
    l.string as laterstring,
    case when o.string = l.string then 0 else 1 end as diffstring,
    o.recordappended1 as origrecordappended1,
    l.recordappended1 as laterrecordappended1,
    case when o.recordappended1 = l.recordappended1 then 0 else 1 end as diffrecordappended1,
    o.recordfloat as origrecordfloat,
    l.recordfloat as laterrecordfloat,
    case when o.recordfloat = l.recordfloat then 0 else 1 end as diffrecordfloat,
    o.recordstring2 as origrecordstring2,
    l.recordstring2 as laterrecordstring2,
    case when o.recordstring2 = l.recordstring2 then 0 else 1 end as diffrecordstring2,
    o.recordboolean2 as origrecordboolean2,
    l.recordboolean2 as laterrecordboolean2,
    case when o.recordboolean2 = l.recordboolean2 then 0 else 1 end as diffrecordboolean2
  FROM (SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime = (
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob`
    WHERE
      _PARTITIONTIME < (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `foo.ar.bob`)
      AND
      _PARTITIONTIME < TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 14 DAY) ) ) o
FULL OUTER JOIN (
  SELECT
     *
  FROM
    `foo.ar.bobdb`
  WHERE
    scantime =(
    SELECT
      MAX(_PARTITIONTIME)
    FROM
      `foo.ar.bob` )) l
ON
    l.integer = o.integer
    AND l.arrayinteger3=o.arrayinteger3
    AND l.arrayfoo=o.arrayfoo
    AND l.arraystring3=o.arraystring3
    AND l.anotherarraytest1=o.anotherarraytest1
    AND l.anotherarraytest2=o.anotherarraytest2
    AND l.string=o.string
    AND l.recordappended1=o.recordappended1
    AND l.recordfloat=o.recordfloat
    AND l.recordstring2=o.recordstring2
    AND l.recordboolean2=o.recordboolean2""",
                                          'description': 'Diff of fortnight of underlying table bob '
                                                         'description: A test schema'}}
        for vi in views:
            expected = vexpected[vi['name']]['query'].splitlines(1)
            actual = vi['query'].splitlines(1)

            diff = difflib.unified_diff(expected, actual)
            diffstr = ''.join(diff)

            print(diffstr)

            self.assertEqual(len(vi['query']), len(vexpected[vi['name']]['query']),
                             "Query len for view {} is not equal to what is expected\n:{}:\n:{"
                             "}: diff{}".format(
                                 vi['name'],
                                 vi['query'],
                                 vexpected[
                                     vi['name']][
                                         'query'],diffstr))
            self.assertEqual(vi['query'], vexpected[vi['name']]['query'],
                             "Query for view {} is not equal to what is expected\n:{}:\n:{"
                             "}:".format(
                                 vi['name'], vi['query'], vexpected[vi['name']]['query']))
            self.assertEqual(vi['description'], vexpected[vi['name']]['description'],
                             "Description for view {} is not equal to what is expected\n:{}:\n:{"
                             "}:".format(
                                 vi['name'], vi['description'],
                                 vexpected[vi['name']]['description']))

    def test_calc_field_depth(self):
        toTest = [{"name": 'string',
                   "type": 'STRING',
                   "description": None,
                   "mode": 'NULLABLE'},
                  {"name": 'integer',
                   "type": 'INTEGER',
                   "description": None,
                   "mode": 'NULLABLE'},
                  {"name": 'float',
                   "type": 'FLOAT',
                   "description": None,
                   "mode": 'NULLABLE'},
                  {"name": 'boolean',
                   "type": 'BOOLEAN',
                   "description": None,
                   "mode": 'NULLABLE'},
                  {"name": 'record',
                   "type": 'RECORD',
                   "description": None,
                   "mode": 'NULLABLE',
                   "fields":
                       [{"name": 'string2',
                         "type": 'STRING',
                         "description": None,
                         "mode": 'NULLABLE'},
                        {"name": 'float',
                         "type": 'FLOAT',
                         "description": None,
                         "mode": 'NULLABLE'},
                        {"name": 'integer2',
                         "type": 'INTEGER',
                         "description": None,
                         "mode": 'NULLABLE'},
                        {"name": 'boolean2',
                         "type": 'BOOLEAN',
                         "description": None,
                         "mode": 'NULLABLE'},
                        {"name": 'record',
                         "type": 'RECORD',
                         "description": None,
                         "mode": 'NULLABLE',
                         "fields":
                             [{"name": 'string2',
                               "type": 'STRING',
                               "description": None,
                               "mode": 'NULLABLE'},
                              {"name": 'record',
                               "type": 'RECORD',
                               "description": None,
                               "mode": 'NULLABLE',
                               "fields":
                                   [{"name": 'string2',
                                     "type": 'STRING',
                                     "description": None,
                                     "mode": 'NULLABLE'
                                    }]
                              }
                             ]
                        }]
                  },
                  {"name": 'array',
                   "type": 'RECORD',
                   "description": None,
                   "mode": 'REPEATED',
                   "fields": [
                       {"name": 'string3',
                        "type": 'STRING',
                        "description": None,
                        "mode": 'NULLABLE'},
                       {"name": 'integer3',
                        "type": 'INTEGER',
                        "description": None,
                        "mode": 'NULLABLE'}
                   ]}
                 ]
        depth = bqtools.calc_field_depth(toTest)
        self.assertEqual(depth, 3, "measured field depth should be 3")
        bqtools.trunc_field_depth(toTest, 2)
        depth = bqtools.calc_field_depth(toTest)
        self.assertEqual(depth, 2, "measured field depth should be 2 is {}".format(depth))
        depth = bqtools.calc_field_depth(self.monsterSchema['schema']['fields'])
        self.assertEqual(depth, 13, "measured field depth should be 13 is {}".format(depth))
        newMonster = copy.deepcopy(self.monsterSchema)
        yamonster = bqtools.trunc_field_depth(newMonster['schema']['fields'], 10)
        depth = bqtools.calc_field_depth(newMonster['schema']['fields'])
        self.assertEqual(depth, 10, "measured field depth should be 10 is {}".format(depth))
        depth = bqtools.calc_field_depth(yamonster)
        self.assertEqual(depth, 10, "measured field depth should be 10 is {}".format(depth))

    def test_run_query(self):
        client = bigquery.client.Client()
        query = """
SELECT word, word_count
FROM `bigquery-public-data.samples.shakespeare`
WHERE corpus = @corpus
AND word_count >= @min_word_count
ORDER BY word_count DESC;
                """
        for row in bqtools.run_query(client, query, logging,
                                        desctext="romeo and juliet",
                                        params={"corpus": "romeoandjuliet",
                                                "min_word_count": 250},
                                        location="US"):
            dict(row)
        query = "SELECT @struct_value AS s;"
        for row in bqtools.run_query(client, query, logging,
                                        desctext="struct",
                                        params={"struct_value": {"x": 1, "y": "foo"}},
                                        location="US"):
            dict(row)
        query = "SELECT TIMESTAMP_ADD(@ts_value, INTERVAL 1 HOUR);"
        for row in bqtools.run_query(client, query, logging,
                                        desctext="datetime",
                                        params={"ts_value": datetime.datetime(2016, 12, 7, 8, 0,
                                                                              tzinfo=pytz.UTC)},
                                        location="US"):
            dict(row)

        query = """
SELECT name, sum(number) as count
FROM `bigquery-public-data.usa_names.usa_1910_2013`
WHERE gender = @gender
AND state IN UNNEST(@states)
GROUP BY name
ORDER BY count DESC
LIMIT 10;
        """
        for row in bqtools.run_query(client, query, logging,
                                        desctext="array",
                                        params={"gender": "M",
                                                "states": ["WA", "WI", "WV", "WY"]},
                                        location="US"):
            dict(row)
        query = """
SELECT * from unnest(@array_name)"""
        for row in bqtools.run_query(client, query, logging,
                                        desctext="array list",
                                        params={"array_name": ["WA", "WI", "WV", "WY"]},
                                        location="US"):
            dict(row)
        query = """
SELECT * from unnest(@array_name)"""
        for row in bqtools.run_query(client, query, logging,
                                        desctext="array dict",
                                        params={"array_name": [{"state": "WA"}, {"state": "WI"},
                                                               {"state": "WV"}, {"state": "WY"}]},
                                        location="US"):
            dict(row)
        query = """
 SELECT * from unnest(?)"""
        for row in bqtools.run_query(client, query, logging,
                                        desctext="array dict positional",
                                        params=[[{"state": "WA"}, {"state": "WI"},
                                                 {"state": "WV"}, {"state": "WY"}]],
                                        location="US"):
            dict(row)

def main(argv):
    unittest.main()


if __name__ == '__main__':
    main(sys.argv)
