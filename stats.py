from datetime import timedelta, datetime
import boto3


s3 = boto3.resource('s3')
cloudwatch = boto3.client('cloudwatch')


def get_buckets(file):
    with open(file, 'r', encoding='utf8') as f:
        bucket_names = f.read().splitlines(False)
    
    buckets = s3.buckets.all()
    targets = []
    for bucket in buckets:
        if bucket.name in bucket_names:
            targets.append(bucket)
            
    return targets

def get_s3_metric_average(bucket_name, metric_name, storage_type, unit='Bytes'):
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/S3',
        MetricName=metric_name,
        Dimensions=[
            {
                'Name': 'BucketName',
                'Value': bucket_name
            },
            {
                'Name': 'StorageType',
                'Value': storage_type
            }
        ],
        StartTime=datetime.utcnow() - timedelta(days=2), # last 2 days. It does not work with 1 and returns an empty result
        EndTime=datetime.utcnow(),
        Period=86400, # 1 day
        Statistics=['Average'],
        Unit=unit
    )
    
    if 'Datapoints' in response and len(response['Datapoints']) > 0:
        return response['Datapoints'][0]['Average']
    else:
        return 0

def get_bucket_size(bucket_name, storage_type='StandardStorage'):
    return get_s3_metric_average(bucket_name, 'BucketSizeBytes', storage_type) / (1024 ** 3)  # bytes -> gigabytes
    
def get_bucket_count(bucket_name):
    return get_s3_metric_average(bucket_name, 'NumberOfObjects', 'AllStorageTypes', 'Count')
        

def tocsv(file, items):
    file.write(','.join([str(i) for i in items]) + '\n')

def generate_stats_csv(buckets, file):
    with open(file, 'w', encoding='utf8') as f:
        tocsv(f, [
            'Bucket Name',
            'Number of Objects',
            'Glacier Overhead (Gb)',
            'Total Size (Gb)',
            'Standard Size (Gb)',
            'Infrequent Access Size (Gb)',
            'Intelligent Tiering Size (Gb)',
            'Glacier Size (Gb)',
        ])
        
        # Metrics reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html#:~:text=dimensions%20in%20CloudWatch-,Amazon%20S3%20daily%20storage%20metrics%20for%20buckets%20in%20CloudWatch,-The%20AWS/S3
        # We get the metrics from cloudwatch, because the s3 api does not provide the number of objects or the size of the bucket.
        for b in buckets:
            print(b.name)
            
            print('   - calculating Standard')
            standard = get_bucket_size(b.name, 'StandardStorage')
            print('   - calculating IA')
            ia = (
                get_bucket_size(b.name, 'StandardIAStorage') +
                get_bucket_size(b.name, 'OneZoneIAStorage')
            )
            print('   - calculating Intelligent Tiering')
            intelligent = (
                get_bucket_size(b.name, 'IntelligentTieringFAStorage') +
                get_bucket_size(b.name, 'IntelligentTieringIAStorage') +
                get_bucket_size(b.name, 'IntelligentTieringAAStorage') +
                get_bucket_size(b.name, 'IntelligentTieringAIAStorage') +
                get_bucket_size(b.name, 'IntelligentTieringDAAStorage')
            )
            print('   - calculating Glacier')
            glacier = (
                get_bucket_size(b.name, 'GlacierInstantRetrievalStorage') +
                get_bucket_size(b.name, 'GlacierStorage') +
                get_bucket_size(b.name, 'DeepArchiveStorage')
            )
            
            print('   - calculating Glacier Overhead')
            overhead = (
                get_bucket_size(b.name, 'GlacierInstantRetrievalSizeOverhead') +
                get_bucket_size(b.name, 'GlacierObjectOverhead') +
                get_bucket_size(b.name, 'DeepArchiveObjectOverhead')
            )
            
            tocsv(f, [
                b.name,
                int(get_bucket_count(b.name)),
                round(overhead, 3),
                round(standard + ia + intelligent + glacier, 3),
                round(standard, 3),
                round(ia, 3),
                round(intelligent, 3),
                round(glacier, 3)
            ])


if __name__ == '__main__':
    buckets = get_buckets('buckets.txt')
    generate_stats_csv(buckets, 'stats.csv')
    
    print('Generated stats for', len(buckets), 'buckets.')  