import boto3
import time
from boto3.dynamodb.conditions import Key, Attr


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Music')


def create_table():
    table = dynamodb.create_table(
        TableName="Music",
        KeySchema=[
            {
                'AttributeName': "Artist",
                'KeyType': 'HASH'
            },
            {
                'AttributeName': "SongTitle",
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Artist',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'SongTitle',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        },
        BillingMode='PROVISIONED'
    )
    table.wait_until_exists()
    return table


def put_item():
    return table.put_item(
        Item={
            'Artist': 'Sid Sriram',
            'SongTitle': 'Nuvvuunte naa jathagha',
            'Year': 2010,
            'Movie': 'I'
        }
    )


def read_item():
    return table.get_item(
        Key={
            'Artist': 'Sid Sriram',
            'SongTitle': 'Nuvvuunte naa jathagha'
        }
    )


def update_item():
    return table.update_item(
        Key={
            'Artist': 'Sid Sriram',
            'SongTitle': 'Nuvvuunte naa jathagha'
        },
        ExpressionAttributeNames={
            '#y': 'Year'
        },
        UpdateExpression='SET #y=:y',
        ExpressionAttributeValues={
            ':y': 2015
        }
    )


def batch_write():
    with table.batch_writer() as batch:
        for i in range(50):
            batch.put_item(
                Item={
                    'Artist': 'Sid Sriram'+str(i),
                    'SongTitle': 'I Love You'+str(i),
                    'Year': 2000+i,
                    'Movie': str(i)
                }
            )
    return table


def query_table():
    response = table.query(
        KeyConditionExpression=Key('Artist').eq('Sid Sriram')
    )
    return response['Items']


def scan_table():
    response = table.scan(
        FilterExpression=Attr('Year').gt(2000)
    )
    return response['Items']


def create_index():
    return boto3.client('dynamodb').update_table(
        TableName="Music",
        AttributeDefinitions=[
            {
                'AttributeName': 'Year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'Movie',
                'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexUpdates=[
            {
                "Create": {
                    "IndexName": "YearIndex",
                    "KeySchema": [
                        {
                            "AttributeName": 'Year',
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "Movie",
                            "KeyType": "RANGE"
                        }
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    },
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5
                    }
                }
            }
        ]
    )


def query_index():
    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
            print('Waiting for index to backfill...')
            time.sleep(30)
            table.reload()
        else:
            break
    response = table.query(
        IndexName="YearIndex",
        KeyConditionExpression=Key('Year').eq(2015)
    )
    return response['Items']


def delete_item():
    return table.delete_item(
        Key={
            'Artist': 'Sid Sriram',
            'SongTitle': 'Nuvvuunte naa jathagha'
        }
    )


def delete_table():
    return table.delete()


if __name__ == '__main__':
    musicTable = create_table()
    print('Table created successfully', musicTable)
    print("Inserted item into table", put_item())
    print("Reading an item from the table", read_item())
    print('Updating the year of music', update_item())
    print("Reading updated item from the table", read_item())
    print('Batch write', batch_write())
    print('Scanned the table', scan_table())
    print('Query the table', query_table())
    print('Create an index', create_index())
    print('Query the index', query_index())
    print('Deleting an item from the table', delete_item())
    print("Deleted table succesfully", delete_table())
