# parallel_s3_copy.py

This script copies the contents (newer than 365 days, or a given retention period) from one S3 bucket to another, in parallel. It is best run from an EC2 instance in the same region as one or both of the buckets.

## Usage

``` bash
python3 parallel_s3_copy.py (-s | --source-bucket) <NAME_OF_BUCKET> (-d | --dest-bucket) [ (-r|--retention) <RETENTION IN DAYS> ] <NAME_OF_BUCKET>
```
