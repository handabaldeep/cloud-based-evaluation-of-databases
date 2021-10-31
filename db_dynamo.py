import boto3
from boto3.dynamodb.conditions import Key
import time


class DynamoDB:
    def __init__(self, region):
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table_name = "ohlc"
        self.table = self.dynamodb.Table(self.table_name)

    def create_table(self):
        if self.table_name not in [table.name for table in self.dynamodb.tables.all()]:
            self.table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'symbol',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'trading_date',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'symbol',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'trading_date',
                        'AttributeType': 'S'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 1000,
                    'WriteCapacityUnits': 1000
                }
            )
            # to allow creation of table
            time.sleep(15)

    def insert(self, symbol_, date_, open_, high_, low_, close_, volume_):
        self.table.put_item(
            Item={
                "symbol": symbol_,
                "trading_date": date_,
                "open_price": str(open_),
                "high_price": str(high_),
                "low_price": str(low_),
                "close_price": str(close_),
                "volume": str(volume_)
            }
        )

    def read(self, symbol_, date_):
        resp = self.table.query(
            ProjectionExpression="volume",
            KeyConditionExpression=Key('symbol').eq(symbol_) & Key('trading_date').lt(date_),
            ScanIndexForward=False,
            Limit=20
        )
        return len(resp["Items"])

    def update(self, symbol_, date_, open_, high_, low_, close_, volume_):
        self.table.update_item(
            Key={
                "symbol": symbol_,
                "trading_date": date_
            },
            UpdateExpression="set open_price=:open_, high_price=:high_, "
                             "low_price=:low_, close_price=:close_, volume=:volume_",
            ExpressionAttributeValues={
                ':open_': str(open_),
                ':high_': str(high_),
                ':low_': str(low_),
                ':close_': str(close_),
                ':volume_': str(volume_)
            },
            ReturnValues="UPDATED_NEW"
        )

    def _cleanup(self):
        self.table.delete()

    def get_size(self):
        if self.table.table_size_bytes:
            return self.table.table_size_bytes
        else:
            return 120

    def close(self):
        self._cleanup()
