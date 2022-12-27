import apache_beam as beam
#from apache_beam.runners.interactive import interactive_runner
#import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners import DataflowRunner
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions, WorkerOptions, StandardOptions
import google.auth
import json
import os

# Settings from Environment Variables.

project_id = os.environ.get('project_id')
source_topic_id = os.environ.get('source_topic_id')
dest_topic_id = os.environ.get('dest_topic_id')
region = os.environ.get('region')
job_id = os.environ.get('job_id')
network = os.environ.get('network')
subnetwork = os.environ.get('subnetwork')
cred = os.environ.get('cred')
key_attrib = os.environ.get('key_attrib')
threshold_value = os.environ.get('threshold_value')
up_threshold_yn = os.environ.get('up_threshold_yn').lower() in ('true', '1', 't')
window_interval = os.environ.get('window_interval')    # 30 sec. Tumbling Window Period. 
repeat_interval = os.environ.get('repeat_interval')    # 10 sec. If this value is greater than 0, Sliding Window Algorithm will be used. 
# For example, 
# window_interval = 10 & repeat_interval = 0. It means this procedure will use Tumbling Window algorithm with 10 second period. 
# window_interval = 20 & repeat_interval = 10. It means this procedure will use Sliding Window algorithm with 20 time spand and every 10 seconds make windows.
dataflow_gcs_temp_location = os.environ.get('temp_location')
dataflow_gcs_stage_location = os.environ.get('stage_location')


#####
# Set Pipeline options.  
options = pipeline_options.PipelineOptions(flags={})

if cred and cred != "" :
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred
  print ("Credential Set")

options.view_as(GoogleCloudOptions).project = project_id
options.view_as(GoogleCloudOptions).region = region
options.view_as(GoogleCloudOptions).job_name = job_id
options.view_as(GoogleCloudOptions).temp_location = dataflow_gcs_temp_location
options.view_as(GoogleCloudOptions).staging_location = dataflow_gcs_stage_location

options.view_as(StandardOptions).runner = 'DataflowRunner'
# Sets the pipeline mode to streaming, so we can stream the data from PubSub.
options.view_as(StandardOptions).streaming = True

options.view_as(WorkerOptions).num_workers = 3
options.view_as(WorkerOptions).network = network
options.view_as(WorkerOptions).subnetwork = "regions/{REGION}/subnetworks/{SUBNETWORK}".format(REGION=region, SUBNETWORK=subnetwork)
# "https://www.googleapis.com/compute/alpha/projects/{PROJECT}/regions/{REGION}/subnetworks/{SUBNETWORK}".format(PROJECT=project_id,
#  REGION=region, SUBNETWORK=subnetwork)
# options.view_as(pipeline_options.SetupOptions).sdk_location = ( '/root/apache-beam-custom/packages/beam/sdks/python/dist/apache-beam-%s0.tar.gz' % beam.version.__version__)

google.auth.default()
##_, options.view_as(GoogleCloudOptions).project = google.auth.default()

#########
## Set Source/Sink Topics
source_topic = "projects/{project}/topics/{topic}".format(project=project_id, topic=source_topic_id)
dest_topic = "projects/{project}/topics/{topic}".format(project=project_id, topic=dest_topic_id)

p = beam.Pipeline(options=options) #runner=interactive_runner.InteractiveRunner(), options=options)

class JsonConverter:
  def __init__(self, key_attrib):
    self.key_attrib = key_attrib
  def kvpair_from_json(self, obj):
    return obj[self.key_attrib], obj
  def bstring_from_objcntpair(self, obj_count):
    return json.dumps(obj_count).encode('utf-8')

class ThresholdFilter:
  def __init__(self, threshold_value, up_yn):
    self.threshold_value = threshold_value
    self.up_yn = up_yn
  def filter(self, txcount):
    if self.up_yn:
      return txcount[1] > self.threshold_value
    else:
      return txcount[1] < self.threshold_value

metrics_jsonb = (p 
  | "Read From PubSub" >> beam.io.ReadFromPubSub(topic=source_topic).with_output_types(bytes)
)
transactions_per_key = ( metrics_jsonb 
  | "Decode from bytes to python dictionary" >> beam.Map(lambda x: json.loads(x.decode('utf-8'))) 
  | "Make pairs with key & object" >> beam.Map(JsonConverter(key_attrib).kvpair_from_json)
)

if repeat_interval > 0:
  window = beam.WindowInto(beam.window.SlidingWindows(window_interval,repeat_interval), 
    trigger=beam.trigger.Repeatedly(beam.trigger.AfterWatermark()), 
    accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
else:
  window = beam.WindowInto(beam.window.FixedWindows(window_interval), 
    trigger=beam.trigger.Repeatedly(beam.trigger.AfterWatermark()), 
    accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)

transaction_count_per_key = (transactions_per_key 
  | "Make Window" >> window 
  | "Count elements per windows & key" >> beam.combiners.Count.PerKey() 
  | "Filter keys with condition" >> beam.Filter(ThresholdFilter(threshold_value, up_threshold_yn).filter) 
  | "Extract only record" >> beam.Map(JsonConverter(key_attrib).bstring_from_objcntpair)
  | "Writing to PubSub" >> beam.io.WriteToPubSub(topic=dest_topic, with_attributes=False)
)

runner = DataflowRunner()
runner.run_pipeline(p, options=options)
