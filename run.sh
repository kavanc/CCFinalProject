#!/bin/bash

# export PROJECT=premium-episode-375722
# export BUCKET=premium-episode-375722-bucket

python predictionsetup.py \
  --runner DataflowRunner \
  --project $PROJECT \
  --staging_location $BUCKET/staging \
  --temp_location $BUCKET/temp \
  --setup_file ./setup.py \
  --input projects/$PROJECT/topics/image_input	\
  --output projects/$PROJECT/topics/prediction \
  --region  northamerica-northeast2 \
  --experiment use_unsupported_python_version \
  --streaming