"""
Beam pipeline implementation

The single function of this module implements a demo Beam batch pipeline.
The pipeline reads hardwired messages from memory and outputs results to
the console and to a local file.

Pipeline outline:

[Read records]
'Read list from memory'
'Filters for youngs'
---- []
     'toUpper name'
     'Prints result'
---- []
     'Translate to Hungarian'
     'Write to file file'

Known issues:
- No logging and only minimal error handling.

Data source:
    - In memory list of dictionary in the form: 
      { 'name': 'Joe', 'age': 12, 'sex': 'm' }

Data sinks:
    - Print to console
    - Local file in directory
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

import transform

# ----------------------------------------------------------------------
# Sample batch pipeline. Not using GCP and running Beam locally.

def run_pipeline( cfg ):
    # --- Initialises the beam pipeline ---
    # args = '--output test.out --runner DirectRunner'
    # options = PipelineOptions(flags=args)
    options = PipelineOptions(flags=None)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = False
    options.view_as(StandardOptions).runner = cfg['beam']['runner']

    # In memory data source for testing. Gender abreviations are
    # intentionally given with mixed key capitalization.
    # Also I use Unicode characters as well.
    people = [
        { 'name': 'Ernő', 'age': 12, 'gender': 'm' },
        { 'name': 'Eve', 'age': 32, 'gender': 'f' },
        { 'name': 'Andrew', 'age': 65, 'gender': 'M' },
        { 'name': 'Mary', 'age': 6, 'gender': 'F' },
        { 'name': 'Ἀβειρὼν', 'age': 12, 'gender': 'm' },
        { 'name': 'Claire', 'age': 77, 'gender': 'F' },
        { 'name': 'آبان بانو', 'age': 12, 'gender': 'F' },
        { 'name': 'Tom', 'age': 40, 'gender': 'M' }
    ]

    # Define and run the pipeline
    with beam.Pipeline(options=options) as p:
        # Main branch of the pipeline
        p0 = ( p 
            | 'Read list from memory' >> beam.Create(people)
            # If reading from local file:
            # lines = p | 'Read from text file' >> beam.io.ReadFromText('data\\sample-input.dat')
            | 'Filters for youngs' >> beam.Filter(lambda e: e['age'] < 20))
        # 1st sub-branch
        p1 = ( p0 
            | 'toUpper name' >> beam.ParDo(transform.ToUpper(), 'name')
            | 'Prints result' >> beam.Map(print))
        # 2nd sub-branch
        p2 = ( p0
            | 'Translate to Hungarian' >> beam.ParDo(transform.TranslateToHun(), 'Férfi', 'Nő')
            | 'Write to file file' >> beam.io.WriteToText(
                    file_path_prefix=cfg['localstore']['file_path_prefix'], 
                    file_name_suffix=cfg['localstore']['file_name_suffix']))

    return

