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

import os
import shutil

import numpy as np
import pandas as pd

from google.cloud import bigquery, storage
from wand.image import Image


def convert_all(input_bucket_name, output_bucket_name):
    """Converts all pdfs in a bucket to png.

    Args:
      input_bucket_name (string): Bucket of Public PDFs
      output_bucket_name (string): Bucket for Converted PNGs
    """

    # Get Images from Public Bucket
    client = storage.Client()
    input_bucket = client.get_bucket(input_bucket_name)
    output_bucket = client.get_bucket(output_bucket_name)

    for blob in client.list_blobs(input_bucket):
        if (blob.name.endswith('.pdf')):

            pdf_basename = os.path.basename(blob.name)
            png_basename = pdf_basename.replace('.pdf', '.png')

            # Download the file to a local temp directory to convert
            temp_directory = "/tmp/google/"
            temp_pdf = os.path.join(temp_directory, pdf_basename)
            temp_png = os.path.join(temp_directory, png_basename)

            input_bucket.get_blob(pdf_basename).download_to_filename(temp_pdf)

            # Convert PDF to PNG
            with Image(filename=temp_pdf, resolution=300) as pdf:
                with pdf.convert('png') as png:
                    png.save(filename=temp_png)

            # Upload to GCS Bucket
            output_bucket.blob(png_basename).upload_from_filename(temp_png)

            # Delete the entire temporary directory
            shutil.rmtree(temp_directory)

# Maybe Write Code to Copy from PDP to Customer Project?


def get_data_from_bq(project_id, dataset_id, output_bucket_name):
    """Fetches Data From BQ Dataset, uploads CSV to Cloud Storage Bucket

    Args:
      project_id (string): Project where dataset is stored
      dataset_id (string): Dataset ID
      output_bucket_name (string): Bucket for Converted PNGs and CSV
    """

    client = bigquery.Client()

    input_bucket_name = "patent_demo_data_pdp"  # Get Bucket Name From PDP

    convert_all(input_bucket_name, output_bucket_name)

    # Extract Table Data into CSV
    for table_ref in client.list_tables(dataset_id, default_project=project_id):

        destination_uri = f"gs://{output_bucket_name}/{table_ref.table_id}.csv"

        table = client.get_table(table_ref)

        # Download CSV to change links to new bucket and pdf to png
        df = client.list_rows(table).to_dataframe()
        df.replace({
            input_bucket_name: output_bucket_name,
            "pdf": "png"
        })
        df.to_csv(destination_uri)


def main():

    project_id = "customer_project"
    dataset_id = "patent_demo_data_pdp"
    output_bucket_name = f"{project_id}-vcm"

    get_data_from_bq(project_id, dataset_id, output_bucket_name)
