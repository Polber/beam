#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

def get_file_from_gcs(path):
  from google.cloud import storage

  # Parse GCS file location
  gcs_file_parts = str(path[5:]).split('/')
  gcs_bucket_name = gcs_file_parts[0]
  gcs_folder = '/'.join(gcs_file_parts[1:])

  # Instantiates a client and downloads file to string
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(gcs_bucket_name)
  blob = bucket.blob(gcs_folder)
  gcs_file = blob.download_as_string()

  return gcs_file