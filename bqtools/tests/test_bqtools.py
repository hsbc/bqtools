# -*- coding: utf-8 -*-
"""
This modules purpose is to test bqtools-json

"""
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import bqtools
import unittest
from deepdiff import DeepDiff
import difflib
import pprint
import copy
import logging
import json

from google.cloud import bigquery


class testScannerMethods(unittest.TestCase):
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

    def test_gendiff(self):
        bqSchema2 = bqtools.create_schema(self.schemaTest2)
        views = bqtools.gen_diff_views('foo', 'ar', 'bob', bqSchema2, description="A test schema")

        vexpected = {'bobdb': {
            "query": """#standardSQL
SELECT
    _PARTITIONTIME AS scantime,
    ifnull(tabob.integer,0) as integer,
    ifnull(A1.integer3,0) as arrayinteger3,
    ifnull(A1.foo,0.0) as arrayfoo,
    ifnull(A1.string3,"None") as arraystring3,
    ifnull(A2.test1,0) as anotherarraytest1,
    ifnull(A2.test2,False) as anotherarraytest2,
    ifnull(tabob.string,"None") as string,
    ifnull(tabob.record.appended1,"None") as recordappended1,
    ifnull(tabob.record.float,0.0) as recordfloat,
    ifnull(tabob.record.string2,"None") as recordstring2,
    ifnull(tabob.record.boolean2,False) as recordboolean2
from `foo.ar.bob` as tabob
LEFT JOIN UNNEST(tabob.array) as A1
LEFT JOIN UNNEST(tabob.anotherarray) as A2""",
            "description": "View used as basis for diffview:A test schema"},
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

            print(''.join(diff))

            self.assertEqual(len(vi['query']), len(vexpected[vi['name']]['query']),
                             "Query len for view {} is not equal to what is expected\n:{}:\n:{"
                             "}:".format(
                                 vi['name'],
                                 vi['query'],
                                 vexpected[
                                     vi['name']][
                                     'query']))
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


def main(argv):
    unittest.main()


if __name__ == '__main__':
    main(sys.argv)