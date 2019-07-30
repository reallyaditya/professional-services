#!/usr/bin/env python

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import automl_v1beta1 as automl
import os


def create_dataset(project_id, compute_region, dataset_metadata, path):
    """Create dataset and import data."""

    client = automl.AutoMlClient()

    # A resource that represents Google Cloud Platform location.
    parent = client.location_path(project_id, compute_region)

    # Create a dataset with the dataset metadata in the region.
    dataset = client.create_dataset(parent, dataset_metadata)

    # Import data from the input URI.
    response = client.import_data(dataset.name, {
        "gcs_source": {
            "input_uris": [path]
        }
    })

    print("Processing import...")

    print(f"Data imported. {response.result()}")
