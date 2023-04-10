import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from beam_nuggets.io import relational_db

import base64

from google.cloud import aiplatform
from google.cloud.aiplatform.gapic.schema import predict

class ParDoFn(beam.DoFn):
    def process(self,
        element, 
        project: str = "modern-kiln-382923",
        endpoint_id: str = "1732808335142420480",
        location: str = "us-central1",
        api_endpoint: str = "us-central1-aiplatform.googleapis.com",
        ):
        # The AI Platform services require regional API endpoints.
    
        client_options = {"api_endpoint": api_endpoint}
        # Initialize client that will be used to create and send requests.
        # This client only needs to be created once, and can be reused for multiple requests.
        client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)

        # The format of each instance should conform to the deployed model's prediction input schema.
        instance = element['car']
        instances = [instance]
        # See gs://google-cloud-aiplatform/schema/predict/params/image_object_detection_1.0.0.yaml for the format of the parameters.
        parameters = predict.params.ImageObjectDetectionPredictionParams(
            confidence_threshold=0.5, max_predictions=5,
        ).to_value()
        endpoint = client.endpoint_path(
            project=project, location=location, endpoint=endpoint_id
        )
        response = client.predict(
            endpoint=endpoint, instances=instances, parameters=parameters
        )
        print("response")
        print(" deployed_model_id:", response.deployed_model_id)
        # See gs://google-cloud-aiplatform/schema/predict/prediction/image_object_detection_1.0.0.yaml for the format of the predictions.
        predictions = response.predictions
        for prediction in predictions:
            return dict(prediction)
class EstimateFn(beam.DoFn):
    def process(self, element):
        coords = [[110, 110, 200, 420], [410, 110, 200, 420], [710, 110, 200, 420]]
        big_c = []
        for c in coords:
            x,y,w,h = c[0], c[1], c[2], c[3]
            big_c.append([x,y,x+w,y+h])
        return big_c
        
def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', dest='input', required=True,
                        help='Input data to process.')
    parser.add_argument('--output', dest='output', required=True,
                        help='Output data to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    
    with beam.Pipeline(options=pipeline_options) as p:

        data = (p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input)
            | "toDict" >> beam.Map(lambda x: json.loads(x)))
        
            
        try:
            c_data = data | 'Predction' >> beam.ParDo(ParDoFn())
            (c_data | 'to byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8')) | 'to Pub/sub' >> beam.io.WriteToPubSub(topic=known_args.output))
        except:
            e_data = data | 'Estimate' >> beam.ParDo(EstimateFn())
            (e_data | 'to byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8')) | 'to Pub/sub' >> beam.io.WriteToPubSub(topic=known_args.output))


            
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()