"""
This module contains the pipeline transformation modules if those are not
implemented as in-line lambda functions within the pipeline itself. ParDo
transformations are implemented as classes, Map transformations are as
functions.

Both the batch (local) and the streaming (GCP) pipelines use transformations
from this module.
"""

import copy                     # for deepcopy
import json                     # for data import
from datetime import datetime   # for timestamp


import jsonschema
import apache_beam as beam

# -----------------------------------------------------------------------
# ParDo function to transform JSON string to dictionary
# We are reporting & omitting ill formatted or semantically wrong records.

e_schema = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "title": "Test JSON input",
  "required": ["name", "age", "gender"],
  "properties": {
    "name": {"type": "string", "default": ""},
    "age": {"type": "integer", "default": 0},
    "gender": {"type": "string", "default": "F", "pattern": "^[fFmM]$"}
	}
}

class Interpret(beam.DoFn):
    def process(self, element):
        try:
            e = json.loads(element)
            jsonschema.validate(e, e_schema)
            yield e
        except json.decoder.JSONDecodeError as err:
            # Poorly-formed text, not JSON
            e_err = {'error': 'Not JSON', 'details': err.msg, 'data': element}
            yield beam.pvalue.TaggedOutput('data_error', e_err )
        except jsonschema.exceptions.ValidationError as err:
            # Message JSON is invalid or missing required attribute
            e_err = {'error': 'Schema error', 'details': err.message, 'data': element}
            yield beam.pvalue.TaggedOutput('data_error', e_err )

# -----------------------------------------------------------------------
# ParDo function that only capitalizes a text dictionary value
# We must not modify the original values, thus creating a new dict.
# Bad practices:
#    e = element                this is wrong as it is only a reference
#    e = copy.copy(element)     still have references for complex elements
# Parameter:
# element as dictionary
# name of the key for which the text value should be capitalized

class ToUpper(beam.DoFn):
    def process(self, element, key_name):
        e = copy.deepcopy(element)      # a real recursive copy
        e[key_name] = e[key_name].upper()
        yield e

# -----------------------------------------------------------------------
# ParDo function to translate dictionary keys and certain values.
# This also demonstrates that side inputs can be used when calling the DoFn.

class TranslateToHun(beam.DoFn):
    def process(self, element, male, female):
        e = { 'Név' : element['name'], 
            'Kor': element['age'], 
            'Nem': (male if element['gender'].upper() == 'M' else female)
            }
        yield e

# -----------------------------------------------------------------------
# Map function to count items in the groups created by GroupBy()
#
# Parameters:
# element is a couple: (<group key>, <list of records in group>)
#   ('F', [{'name': 'Eve', 'age': 32, 'gender': 'F'}, {'name':...}, ...])
# key_name is the dictionary key name of te group as 'gender'
#
# Result:
# List of a single dictionary with the key and count values
#   [{ 'gender': 'F', 'count': 1233 }] or [{ 'gender': 'M', 'count': 576 }]

def CountGroups(element, key_name='group'):
        (key, records) = element
        e = { key_name: key, 'count' : len(records) }
        return e

# -----------------------------------------------------------------------
# ParDo function to add window info to the record

class AddWindowInfo(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        e = copy.deepcopy(element)
        ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
        e['window_start'] = window.start.to_utc_datetime().strftime(ts_format)
        e['window_end'] = window.end.to_utc_datetime().strftime(ts_format)
        yield e

# -----------------------------------------------------------------------
# ParDo function to add process time timestamp to the record

class AddTimestamp(beam.DoFn):
    def process(self, element):
        e = copy.deepcopy(element)
        ts_format = '%Y-%m-%d %H:%M:%S UTC'
        dt = datetime.utcnow()
        e['processed'] = dt.strftime(ts_format)
        yield e

