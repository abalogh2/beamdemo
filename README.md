# Apache Beam basic capabilities demonstration

## Introduction

This Python project is demonstrating the basic capabilities of Beam 
integrated with the following Google Cloud Platform (GCP) services:

- Pub/Sub
- BigQuery
- Storage

The application is receiving very simple records with name, age and 
gender and performs various transformation on them long a Beam
pipeline. The Beam application itself is running on the local development 
computer. Using the GCP DataFlow version is not demonstrated here,
though porting the complete solution to the GCP environment should 
not be difficult.

The "local" version is reading the hard-wired input from memory (source)
and reporting the results in a local file and on the console.

The "gcp" version is reading the messages from a Pub/Sub topic and 
stores the results and error messages in BigQuery tables. It also
writes the results to a Storage bucket, but due to system limitations
that solution is not complete (see remarks at the end of this doc).

Input data format is JSON:
```
{"name": "Joe", "age": 22, "gender": "M"}
```

Demonstrated features:

- Setting up Python virtual environment with appropriate modules
- Configuring a GCP environment and generating credential file
- Running Beam locally (DirectRunner)
- Setting up complex, branching Beam pipelines
- Using bounded (batch) and unbounded (streaming) modes
- Reading messages from Pub/Sub topic and from local memory
- Checking messages via JSON templates
- Using standard and custom ParDo, Map transforms with dedicated
  functions or with inline lambda functions.
- Using side inputs with ParDo routines
- Using multiple outputs with ParDo routines for error handling
- Using windowing for handling aggregations in streaming mode

Implementing this demo might involve some costs on Google Cloud Platform, 
however it will not be more than a few dollars (possibly less than a dollar).
See the Clean-up chapter at the end to delete all GCP resources after
playing with this demo.

Those who have some basic experience with Python and GCP could implement
and run this demo application within half an hour.

## Implemented pipelines

The simpler pipeline is not using any GCP functionalities. It reads hardwired
data from memory and outputs the results on the console and into a local file.
The data are processed in batch mode.

![Local batch pipeline](https://github.com/ghrasko/beamdemo/blob/master/pics/local-flow.jpg)

The more complex second pipeline is fetching the data from a Pub/Sub topic
and implementing a streaming pipeline to process them. Outputs are sent to
the console, to various BigQuery tables and to Storage bucket files.

![GCP streaming pipeline](https://github.com/ghrasko/beamdemo/blob/master/pics/gcp-flow.jpg)

The BigQuery output tables are as follows:

- Age based filtered messages are stored in *beam.filtered_messages* table.
- Aggregated results (counts by gender) are stored in *beamdemo.gender_counts* table.
Aggregation records are stored for each time window whenever data arrives.
- Message related error information (bad format etc.) is stored in *beamdemo.error.log* table.

## Basic directories

We use two main directories during the demonstration:

- Python virtual environments root: C:\Python
- Python project with source files: C:\Dev\Beam

Your directories might be different. Replace the directory names used
in this document with your ones.

## Getting the project code

https://github.com/ghrasko/beamdemo

## Setting up the GCP environment

Perform the below configurations on the [Google Cloud Console](https://console.cloud.google.com/).
Project name and bucket name should be globally unique. 
Replace xxxxxxxx with a unique string, perhaps with your user name.

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
- apache-beam\[gcp\]
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

Activate the proper python virtual environment
```
>C:\Python\beam\Scripts\activate
```
For testing the app without GCP features run as follows
```
> (beam) cd C:\dev\beam
> (beam) python subdemo.py --cfgfile config.json --pipeline local
```
For testing the app with the GCP features run as follows
```
> (beam) python subdemo.py --cfgfile config.json --pipeline gcp
```
Send test messages via the [gcloud shell](https://cloud.google.com/sdk/gcloud/reference/pubsub/topics/publish). 
Paste the below list to the cloud shell. Messages might also be added manually on-by-one through the GCP 
Pub/Sub Console for further testing.

The prepared list below contains 11 valid and 6 invalid messages. 6 of the 11 valid inputs are 
about people younger than 20 years, so they will be included in the filtered list. 
The gender ratio is 3:3. This will be reported in the aggregated output. The
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
Check BigQuery data tables by runing the following queries:

```
select * from beamdemo.filtered_messages order by processed desc limit 20
```
![Contents of my filtered_messages table on BigQuery](https://github.com/ghrasko/beamdemo/blob/master/pics/bq-filtered-messages.jpg)
```
select * from beamdemo.gender_counts order by window_start desc limit 20
```
![Contents of my gender_counts table on BigQuery](https://github.com/ghrasko/beamdemo/blob/master/pics/bq-gender-counts.jpg)
```
select * from beamdemo.error_log order by processed desc limit 20
```
![Contents of my error_log table on BigQuery](https://github.com/ghrasko/beamdemo/blob/master/pics/bq-error-log.jpg)

After completing demoing, halt the application in the console by pressing Ctrl-C.

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
  single, final (not temporary) file. The [documentation writes](https://beam.apache.org/documentation/sdks/python-streaming/):
  *'The Beam SDK for Python includes two I/O connectors that support 
  unbounded PCollections: Google Cloud Pub/Sub (reading and writing) and 
  Google BigQuery (writing).'*
- id_label argument is not (yet?) supported in PubSub reads (for Python?)

## Further improvement possibilities

The following improvements could be made on this demonstration:

- Move Beam to GCP (DataFlow)
- More error handling
- Shifted windowing
- Late data handling (requires source timestamped data and simulated delays)
- Graceful termination (draining)
- Pipeline options handling should be modernized (see warning when running the code in the console)


