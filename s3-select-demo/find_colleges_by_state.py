import boto3

S3_BUCKET = 'themedius.ai'

s3 = boto3.client('s3')

r = s3.select_object_content(
    Bucket=S3_BUCKET,
    Key='COLLEGE_DATA.csv',
    ExpressionType='SQL',
    Expression="select \"INSTNM\" from s3object s where s.\"STABBR\" in ['OR', 'IA']",
    InputSerialization={'CSV': {"FileHeaderInfo": "Use"}},
    OutputSerialization={'CSV': {}},
)

for event in r['Payload']:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8')
        print(records)
