
import lambda_function_test

event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49630676635694299160546996848896052648183249925362941954",
                "data": "ewogICAgICAgICJyaWRlIjogewogICAgICAgICAgICAiUFVMb2NhdGlvbklEIjogMTMwLAogICAgICAgICAgICAiRE9Mb2NhdGlvbklEIjogMjA1LAogICAgICAgICAgICAidHJpcF9kaXN0YW5jZSI6IDMuNjYKICAgICAgICB9LCAKICAgICAgICAicmlkZV9pZCI6IDI1NgogICAgfQ==", 
                "approximateArrivalTimestamp": 1655835394.023
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49630676635694299160546996848896052648183249925362941954",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::849352486600:role/lambda-kinesis-role",
            "awsRegion": "us-east-1",
            "eventSourceARN": "arn:aws:kinesis:us-east-1:849352486600:stream/ride_events"
        }
    ]
}
result = lambda_function_test.lambda_handler(event, None)
print(result)