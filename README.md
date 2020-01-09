# Apache Beam basic capabilities demonstration

## Introduction

Thy Python project is demonstrating the basic capabilities of Beam 
integrated with the following Google Cloud Platform (GCP) services:

- Pub/Sub
- BigQuery
- Storage

The Beam application itself is running on the local development 
computer. Using the GCP DataFlow version is not demonstrated here,
though porting the complete solution to the GCP environment should 
not be difficult.

The "local" version is reading the hard-wired input from memory (source)
and reporting the results in a local file and on the console.

The "gcp" version is reading the messages from a Pub/Sub topic and 
stores the results and error messages in BigQuery tables. It also
writes the results to a Storage bucket, but due to system limitations
that solution is not complete (see remarks at the end of this doc).

Demonstrated features:

- Setting up Python virtual environment with appropriate modules
- Configuring a GCP environment and generating credential file
- Running Beam locally (DirectRunner)
- Setting up complex Beam pipelines
- Using bounded (batch) and unbounded (streeming) modes
- Reading messages from Pub/Sub topic and from local memory
- Checking messages via JSON templates
- Using standard and custom ParDo, Map and lambda function transforms
- Using side inputs with ParDo routines
- Using multiple outputs with ParDo routines for error handling
- Using windowing for handling aggregations in streaming mode

Implementing this demo might involve some costs on Google Cloud Platform, 
however it will not be more than a few dollars (possibly less than a dollar).
See the Clean-up chapter at the end to delete all GCP resources after
playing with this demo.

## Basic directories

We use two main directories during the demonstration:

- Python virtual environments root: C:\Python
- Python project with source files: C:\Dev\Beam

Your directories might be different. Replace the directory names used
in this document with your ones.

## Getting the project code

https://github.com/ghrasko/beamdemo

## Setting up the GCP environment

(Project name and bucket name should be globally unique. 
Replace xxxxxxxx with a unique string, perhaps with your user name)

```
Create new project (should be globally unique): 
   beamdemo-xxxxxxxx
APIs and Services enable API:
   Cloud Pub/Sub API
Pub/Sub Create topic: 
   beamdemo
Storage Create bucket: 
   beamdemo-xxxxxxxx
Storage Create folder: 
   beamdemo-xxxxxxxx/binaries
Storage Create folder: 
   beamdemo-xxxxxxxx/temp
BigQuery Create dataset in beam-test-xxxxxxxx resource:
   beamdemo
In IAM & Admin | Service accounts menu create service account:
   Name: beamdemo
   Role: Project/Owner   (should be much more restrictive in production)
   Create key (JSON) and save it to a folder not managed by Git:
      ../.cred/beamdemo
```

## Setting up Python environment

We are installing the solution on a Windows 10 PC and using the standard
venv package managing virtual environments in Python. Our base Python version
is 3.7.5. On Unix environments and with alternative virtual environment 
managers the actual commands might be slightly different.

We are using only two non-standard Python packages:
- apache-beam[gcp]
- jsonschema

The installation commands are as follows:

```
> C:
> cd C:\Python
> python --version
Python 3.7.5
> python -m venv beam
> beam\Scripts\activate
> (beam) pip --version
pip 19.2.3 from c:\python\beam\lib\site-packages\pip (python 3.7)
> (beam) python -m pip install --upgrade pip
> (beam) pip --version
pip 19.3.1 from c:\python\beam\lib\site-packages\pip (python 3.7)
> (beam) pip install -r requirements.txt
```

## Configuration file settings

```
Copy config_sample.json to config.json
Edit config.json with the configured GCP parameters
```

## Running the demo

[Activate the proper python virtual environment]
```
>C:\Python\beam\Scripts\activate
```
[For testing the app without GCP features run as follows]
```
> (beam) cd C:\dev\beam
> (beam) python subdemo.py --cfgfile config.json --pipeline local
```
[For testing the app with the GCP features run as follows]
```
> (beam) python subdemo.py --cfgfile config.json --pipeline gcp
```
Sending test message via the gcloud shell. Paste the below list to the cloud shell. 
(https://cloud.google.com/sdk/gcloud/reference/pubsub/topics/publish)

The list contains 11 valid and 6 invalid messages. 6 of the 11 valid inputs are 
about people younger than 20 years, so they will be included in the filtered list. 
The gender ration is 3:3. This will be reported in the aggregated output. The
messages will probably arrive during a single 1 minute window, though this is not
certain as windows start at round minutes and not when the first message arrives.
```
gcloud pubsub topics publish beamdemo --message '{ "name": "Ernő", "age": 12, "gender": "m" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Eve", "age": 32, "gender": "f" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Andrew", "age": 65, "gender": "M" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Mary", "age": 6, "gender": "F" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Ἀβειρὼν", "age": 12, "gender": "m" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Claire", "age": 77, "gender": "F" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "آبان بانو", "age": 12, "gender": "F" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Tom", "age": 40, "gender": "M" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Mary", "age": "6", "gender": "F" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Mary", "age": 6, "gender": "female" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Mary", "age": 6, "sex": "F" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Mary", "age": 6, "sex": "F" }{ "name": "Mary", "age": 6, "sex": "F" }'
gcloud pubsub topics publish beamdemo --message 'Hello World.'
gcloud pubsub topics publish beamdemo --message '{ "name": "Ярослав'
gcloud pubsub topics publish beamdemo --message '{ "name": "Ярослав", "age": 19, "gender": "m" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "София", "age": 10, "gender": "F" }'
gcloud pubsub topics publish beamdemo --message '{ "name": "Arsène", "age": 42, "gender": "m" }'
```
Messages might also be added manually on-by-one through the GCP Pub/Sub Console.

Check BigQuery data tables by runing the following queries:
```
select * from beamdemo.filtered_messages order by processed desc limit 20
select * from beamdemo.gender_counts order by window_start desc limit 20
select * from beamdemo.error_log order by processed desc limit 20
```
Stop the application in the console by pressing Ctrl-C.

## Clean-up

Delete the test GCP project to avoid any further costs. At the cloud shell issue
the following command (https://cloud.google.com/sdk/gcloud/reference/projects/delete):
```
gcloud projects delete beamdemo-xxxxxxxx
```
## Known issues

- beam.io.WriteToText() is not (yet?) suported for streaming in Python.
  Output is written correctly to temp files in temp directories. It is
  not clear logically how this should be working while streaming. Anyhow
  I was expecting that at least the output is written sequentially in a
  single, final (not temporary) file. See this:
  https://beam.apache.org/documentation/sdks/python-streaming/
  'The Beam SDK for Python includes two I/O connectors that support 
  unbounded PCollections: Google Cloud Pub/Sub (reading and writing) and 
  Google BigQuery (writing).'
- id_label argument is not (yet?) supported in PubSub reads (for Python?)

## Further improvement possibilities

The following improvements could be made on this demonstration:

- Move Beam to GC (DataFlow)
- More error handling. See for example:
  https://beam.apache.org/contribute/ptransform-style-guide/#error-handling
- Shifted windowing
- Late ticket handling
- Graceful termination (draining)
- Pipeline options handling should be modernized (see warning when running)


