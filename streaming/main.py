import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            yield data
        except json.JSONDecodeError:
            pass

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project

    with beam.Pipeline(options=pipeline_options) as p:
        events = (
            p
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription,
                with_attributes=False
            )
            | 'Parse JSON' >> beam.ParDo(ParseEvent())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema='event_type:STRING,user_id:STRING,amount:FLOAT,timestamp:TIMESTAMP',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()