"""
BEAM PIPELINE (streaming, GCP)

[Read records]
'Read from PubSub'
'Parse JSON to Dict'
---- [Handle bad records]
     'Add timestamp to error'
     'Store error in BigQuery' (sink)
---- [Filter records]
     'Filters for youngs'
     ---- [Write filtered raw records to console]
          'Translate to Hungarian'
          'Prints msg to console' (sink)
     ---- [Write filtered raw records to BigQuery table]
          'Add timestamp to msg'
          'Save msg to BigQuery' (sink)
     ---- [Calculate aggregate measure (count) on gender field]
          'Define windowing'
          'toUpper gender'
          'Define gender as key'
          'Group by gender field'
          'Count group members'
          'Add window info'
          ---- [Save windowed aggregate results to BigQuery table]
               'Save sum to BigQuery' (sink)
          ---- [Print windowed aggregate results to console]
               'Prints sum to console' (sink)
          ---- [Save windowed aggregate results to Storage bucket]
               'Write sum to GC file' (sink)
 
Data source:
    - Pub/Sub messages in the form: 
      { 'name': 'Joe', 'age': 12, 'gender': 'm' }

Data sinks:
    - Print to console
    - GC BigQuery tables
    - GC Storage file in bucket (but see known issues)
"""

import os

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

import transform    # Beam transform step codes

# ----------------------------------------------------------------------
# Sample stream pipeline using GC for source and sink. Runing Beam locally. 

def run_pipeline( cfg ):
    # Set ENV variable with Google Cloud credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cfg['gc']['cred_file']

    # --- Initialises the beam pipeline ---
    # options = PipelineOptions(flags=None)
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = cfg['beam']['runner']

    # --- When using GC, the relevant parameters should be set ---
    gc_options = options.view_as(GoogleCloudOptions)
    gc_options.job_name = cfg['gc']['job']
    gc_options.staging_location = cfg['gc']['staging_location']
    gc_options.temp_location = cfg['gc']['temp_location']

    # Define and run the pipeline
    with beam.Pipeline(options=options) as p:
        # Main branch of the pipeline
        p0 = ( p 
            # If only topic is given, temporary subscription is automatically generated
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(
                topic='projects/{}/topics/{}'.format(
                    cfg['gc']['project'],
                    cfg['pubsub']['topic']))
            # If an exsisting subscription is defined, topic is inferred 
            # | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=topic_path)
            | 'Parse JSON to Dict' >> beam.ParDo(transform.Interpret())
                    .with_outputs('data_error', main='data_ok')
        )
        # Input error handling. Might use p0['data_error'] format also.
        p0_err = ( p0.data_error        
            | 'Add timestamp to error' >> beam.ParDo(transform.AddTimestamp())
            | 'Store error in BigQuery' >> beam.io.WriteToBigQuery(
                   cfg['bigquery']['table_err'], 
                   dataset=cfg['bigquery']['dataset'], 
                   project=cfg['gc']['project'],
                   schema=cfg['bigquery']['schema_err'], 
                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                   write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )
        # Go on with correct input. Might use p0['data_ok'] format also.
        p0_ok = ( p0.data_ok
            # Process only people with age less then 20 
            | 'Filters for youngs' >> beam.Filter(lambda e: e['age'] < 20)
        )
        # 1st sub-branch: print filtered raw messages in Hungarian
        p1 = ( p0_ok
            | 'Translate to Hungarian' >> beam.ParDo(transform.TranslateToHun(), 'Férfi', 'Nő')
            | 'Prints msg to console' >> beam.Map(print)
        )
        # 2nd sub-branch: save filtered raw messases to BigQuery table
        p2 = ( p0_ok
            | 'Add timestamp to msg' >> beam.ParDo(transform.AddTimestamp())
            | 'Save msg to BigQuery' >> beam.io.WriteToBigQuery(
                   cfg['bigquery']['table2'], 
                   dataset=cfg['bigquery']['dataset'], 
                   project=cfg['gc']['project'],
                   schema=cfg['bigquery']['schema2'], 
                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                   write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )
        # 3rd sub-branch: groupping by gender and reporting it in 3 ways
        p3 = ( p0_ok
            | 'Define windowing' >> beam.WindowInto(window.FixedWindows(
                    cfg['beam']['window_length'], cfg['beam']['window_offset']))
            | 'toUpper gender' >> beam.ParDo(transform.ToUpper(), 'gender')
            | 'Define gender as key' >> beam.Map(lambda e: (e['gender'], e))
            # This would combine the previous two steps into a single one
            #| 'Define gender as key' >> beam.Map(lambda e: (e['gender'].upper(), e))
            | 'Group by gender field' >> beam.GroupByKey()
            | 'Count group members' >> beam.Map(transform.CountGroups, 'gender')
            | 'Add window info' >> beam.ParDo(transform.AddWindowInfo())
        )
        # 3.1 sub-branch: report to BigQuery 
        p3_1 = ( p3
            | 'Save sum to BigQuery' >> beam.io.WriteToBigQuery(
                    cfg['bigquery']['table1'], 
                    dataset=cfg['bigquery']['dataset'], 
                    project=cfg['gc']['project'],
                    schema=cfg['bigquery']['schema1'], 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )
        # 3.2 sub-branch: report to condole
        p3_2 = ( p3
            | 'Prints sum to console' >> beam.Map(print)
        )
        # 3.3 sub-branch: report to GC Store file
        p3_3 = ( p3
            # Writing to file either locally or into a GC bucket seems not to be supported yet
            # for Python when streaming. It is only outputting the individual temp files.
            | 'Write sum to GC file' >> beam.io.WriteToText(
                    cfg['storage']['file_path_prefix'], 
                    file_name_suffix=cfg['storage']['file_name_suffix'], 
                    append_trailing_newlines=cfg['storage']['append_newline'])
        )

    # Alternative syntaxes for defining and running the pipeline
    # Version 1:
    # with beam.Pipeline(options=options) as p:
    #    p0 = (p | ...
    #    ...
    #
    # Version 2: (but proper cleanup of resources should be chekced)
    # p = beam.Pipeline(options=options)
    #     p0 = (p | ...
    #     ...
    # result = p.run()
    # result.wait_until_finish()
    # result.cancel()

    return

