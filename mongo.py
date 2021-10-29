import pymongo


class Mongo:
    def __init__(self, host, port, user, password, database):
        self.mongodb = pymongo.MongoClient(
            host=host,
            port=port,
            username=user,
            password=password,
            retryWrites=False
        )
        self.database = self.mongodb[database]
        self.table_name = "ohlc"
        self.table = None

    def create_table(self):
        if self.table is None:
            self.table = self.database[self.table_name]

    def insert(self, symbol_, date_, open_, high_, low_, close_, volume_):
        self.table.insert_one(
            {
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
        resp = self.table.find(
            {
                "symbol": symbol_,
                "trading_date": {"$gte": date_}
            }
        )
        res = [x for x in resp]
        return len(res)

    def update(self, symbol_, date_, open_, high_, low_, close_, volume_):
        self.table.update_one(
            {
                "symbol": symbol_,
                "trading_date": date_
            },
            {
                "$set": {
                    'open': str(open_),
                    'high': str(high_),
                    'low': str(low_),
                    'close': str(close_),
                    'volume': str(volume_)
                }
            }
        )

    def _cleanup(self):
        self.database.drop_collection(self.table_name)

    def get_size(self):
        return self.database.command("dbstats")["storageSize"]/(1024*1024)

    def close(self):
        self._cleanup()
        self.mongodb.close()
