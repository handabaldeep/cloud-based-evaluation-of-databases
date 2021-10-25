import boto3
from boto3.dynamodb.conditions import Key
import time


class Dynamo:
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
            time.sleep(10)

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
            KeyConditionExpression=Key('symbol').eq(symbol_) & Key('trading_date').gte(date_)
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
            return 111

    def close(self):
        self._cleanup()


if __name__ == "__main__":
    dynamo = Dynamo(region="eu-west-2")
    dynamo.create_table()
    dynamo.insert("a", "2011-01-01", 111.22, 111.4444, 111.1, 111.333, 1234509876)
    print(dynamo.read("a", "2011-01-01"))
    dynamo.update("a", "2011-01-01", 121.22, 143.4444, 113.1, 133.333, 1234567890)
    print(dynamo.get_size())
    #dynamo.close()
