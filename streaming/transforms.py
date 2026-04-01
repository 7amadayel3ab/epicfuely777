import json
import apache_beam as beam

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            # You can add validation or enrichment here
            yield data
        except json.JSONDecodeError:
            # Optionally log error
            pass