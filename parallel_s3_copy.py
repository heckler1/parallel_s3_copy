#!/usr/bin/env python3
"""
This script moves the contents of one bucket to another.
We use Python+Boto3 instead of the AWS CLI because moving the sheer number of objects
in some of our buckets is better suited to the real API than the abstracted CLI.
This is mainly due to speed and credential timeout issues.

For speed reasons, we also parallelize the copy function.
Due to each copy being in a new thread, this does not support MFA,
as it will spam ${CORE_COUNT} number of MFA prompts onto the screen
all at once when it spins up the parallelizer.
"""
import datetime
import multiprocessing
import argparse
import boto3
import joblib

# Get arguments
parser = argparse.ArgumentParser()
parser.add_argument(
  "-s",
  "--source-bucket",
  required=True,
  help="The source bucket to copy from"
)
parser.add_argument(
  "-d",
  "--dest-bucket",
  required=True,
  help="The destination bucket to copy to"
)
parser.add_argument(
  "-r",
  "--retention",
  required=False,
  default=365,
  help="Maximum age of files to copy, in days. Default: 365 days"
)
args = parser.parse_args()
source_bucket_name = args.source_bucket
dest_bucket_name = args.dest_bucket
retention = args.retention

# Find out what date a year ago is
today = datetime.datetime.now(datetime.timezone.utc)
year_old = today - datetime.timedelta(days=retention)

# Instantiate our S3 resources
# I know we're using a resource and a client,
# but its easier and nicer to get the list of "s3.ObjectSummary" items from the S3 resource.
# And it's faster to use the S3 client to actually copy the objects.
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
# List the objects in the source bucket
objects = s3_resource.Bucket(source_bucket_name).objects.all()

# Get the number of threads we can run concurrently
num_cores = multiprocessing.cpu_count()

def s3_copy(last_modified, key):
  """
  This function copies an S3 object from one bucket to another,
  if it's less than 1 year old.

  It references the variables defined at the top of this script for configuration

  These parameters are attributes of the "s3.ObjectSummary" given by our bucket listing.
  The ObjectSummary cannot be passed through directly (pickle can't serialize it),
  so we pass the attributes instead

  :param last_modified
    The last modified date of the object
  :param key
    The key of the object
  """
  # If the item is 1 year old, or newer, copy it to the destination bucket
  if last_modified >= year_old:
    # Copy the item, maintaining the same file path
    s3_client.copy_object(
      Bucket=dest_bucket_name,
      Key=key,
      CopySource={
        'Bucket': source_bucket_name,
        'Key': key
      }
    )

    print("Copied " + key + " from " + source_bucket_name + " to " + dest_bucket_name)
    # Delete it from the old bucket
    # Enabling this would effectively do a move instead of a copy
    '''dest_bucket.delete_objects(
      Delete={
        'Objects': [
          {
            'Key': key
          }
        ]
      }
    )'''

# When this script is run from the command line
if __name__ == "__main__":
  # Use the multiprocessing backend because loky does not like SSLContext objects
  with joblib.parallel_backend('multiprocessing'):
    # Run as many jobs as we have cores
    joblib.Parallel(n_jobs=num_cores)(
      # Queue up the jobs for the parallelizer
      joblib.delayed
      # Run the S3 copy function with the last modified date and key of our objects
      (s3_copy)(item.last_modified, item.key)
      for item in objects
    )
