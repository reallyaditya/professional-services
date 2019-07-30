#!/usr/bin/env python

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import consts
import prepare_data
import init_automl

# Export BQ Data to CSV, replace filenames and upload to cloud storage bucket
prepare_data.bq_to_csv(
    consts.PROJECT_ID, consts.DATASET_ID, consts.INPUT_BUCKET, consts.OUTPUT_BUCKET)

# # Convert all PDFs in input bucket to PNGs in output bucket
prepare_data.convert_pdfs(
    consts.INPUT_BUCKET, consts.OUTPUT_BUCKET, consts.TEMP_DIRECTORY)

dataset_metadata = {
    "display_name": consts.DATASET_ID,
    "image_classification_dataset_metadata": {
        "classification_type": "MULTICLASS"
    }
}

# TODO run for each csv
path = "gs://pdf-processing-219114-vcm/holt/entity_extraction.csv"
init_automl.create_dataset(
    consts.PROJECT_ID, consts.REGION, dataset_metadata, path)
