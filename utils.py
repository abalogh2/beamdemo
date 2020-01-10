"""
This module performs configuration file reading with checking against 
defined schema. On config file errors it halts the program with 
appropriate error message.
"""

import json         # for config file load
import jsonschema   # for checking config file content

# Config file schema definition.
# Generated from sample config file by: https://www.jsonschema.net/
schema = {
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object", "title": "Python Beam demo config file",
  "required": ["localstore", "gc", "beam", "storage", "pubsub", "bigquery"],
  "properties": {
    "localstore": {
      "type": "object", "title": "PC storage options",
      "required": ["file_path_prefix", "file_name_suffix"],
      "properties": {
        "file_path_prefix": {"type": "string"},
        "file_name_suffix": {"type": "string"}
      }
    },
    "gc": {
      "type": "object", "title": "Google Cloud related parameters",
      "required": ["cred_file", "project", "job", "staging_location", "temp_location"],
      "properties": {
        "cred_file": {"type": "string"},
        "project": {"type": "string"},
        "job": {"type": "string"},
        "staging_location": {"type": "string"},
        "temp_location": {"type": "string"}
      }
    },
    "beam": {
      "type": "object", "title": "Beam (DataFlow) related parameters",
      "required": ["runner", "save_main_session", "window_length", "window_offset"],
      "properties": {
        "runner": {"type": "string", "default": "DirectRunner", "examples": ["DirectRunner","DataflowRunner"]},
        "save_main_session": {"type": "boolean", "default": True},
        "window_length": {"type": "integer", "default": 60},
        "window_offset": {"type": "integer", "default": 0}
      }
    },
    "storage": {
      "type": "object", "title": "GC Storage related parameters",
      "required": ["file_path_prefix", "file_name_suffix", "append_newline"],
      "properties": {
        "file_path_prefix": {"type": "string", "examples": ["gs://mystorage/mybucket"]},
        "file_name_suffix": {"type": "string", "default": "", "examples": [".lst", ".dat"]},
        "append_newline": {"type": "boolean", "default": False}
      }
    },
    "pubsub": {
      "type": "object", "title": "PubSub related parameters",
      "required": ["topic"],
      "properties": {
        "topic": {"type": "string", "examples": ["mytopic", "beamtest"]}
      }
    },
    "bigquery": {
      "type": "object", "title": "BigQuery related parameters",
      "required": ["dataset", "table1", "schema1", "table2", "schema2", "table_err", "schema_err"],
      "properties": {
        "dataset": {"type": "string", "examples": ["mydataset"]},
        "table1": {"type": "string", "examples": ["people", "jobs"]},
        "schema1": {"type": "string", "examples": ["gender:STRING, count:INTEGER"]},
        "table2": {"type": "string", "examples": ["people", "jobs"]},
        "schema2": {"type": "string", "examples": ["name:STRING, gender:STRING, age:INTEGER"]},
        "table_err": {"type": "string", "examples": ["people", "jobs"]},
        "schema_err": {"type": "string", "examples": ["error:STRING, data:INTEGER"]}
      }
    }
  }
}

# --------------------------------------------------------------------
# Terminates the program on error

def error_exit( msg, error=None ):
    auxinfo = f': {error}' if error else ''
    print(f'ERROR: {msg}{auxinfo}')

    exit(-1)

# --------------------------------------------------------------------
# Reads config data from JSON file

def read_config(filename):

    try:
        with open(filename, 'rt') as f:
            cfg = json.load(f)
            jsonschema.validate(cfg, schema)

    except OSError as e:
        error_exit('Config file not found', e.filename)
    except json.decoder.JSONDecodeError as e:
        error_exit('Poorly-formed text, not JSON:', e)
    except jsonschema.exceptions.ValidationError as e:
        error_exit('Config JSON is invalid or missing required attribute.', e)
    # This must not happen once the schema itself is tested.
    # https://python-jsonschema.readthedocs.io/en/stable/errors/#jsonschema.exceptions.SchemaError
    except jsonschema.exceptions.SchemaError as e:
        error_exit('The JSON schema is invalid.', e)

    return cfg
