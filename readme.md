# Bucket Stats

Storage statistic generation tool for AWS S3 buckets with storage classes

## Usage

### 1. Authenticate to AWS

Log into your AWS account using the AWS CLI. This depends on your use-case so I won't go into detail here.

### 2. Create a list of buckets

With any tool you are comfortable with, create a list of S3 bucket names you'd like to generate statistics for. The list should be a text file with one bucket name per line.

```
bucket1
bucket2
bucket3
```

> Make sure it's just the name, not the URL (`s3://...`) or ARN (`arn:aws:s3:::...`)

### 3. Install dependencies

```bash
make install
```

*or*

```bash
pip install -r requirements.txt
```

### 4. Run the script

```bash
python stats.py temp/bucket.txt temp/stats.csv
```

## Docs

Metrics and Dimensions docs for S3: https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html
