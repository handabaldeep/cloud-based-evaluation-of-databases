import pymongo
import datetime


class MongoDB:
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
                "_id": f"{symbol_}_{date_}",
                "symbol": symbol_,
                "trading_date": datetime.datetime.strptime(date_, "%Y-%m-%d"),
                "open_price": open_,
                "high_price": high_,
                "low_price": low_,
                "close_price": close_,
                "volume": volume_
            }
        )

    def read(self, symbol_, date_):
        resp = self.table.find(
            {
                "_id": {"$lt": f"{symbol_}_{date_}"},
                "symbol": symbol_
            },
            {
                "volume"
            }
        ).sort(
            [("_id", -1)]
        ).limit(20)
        res = [x for x in resp]
        return len(res)

    def update(self, symbol_, date_, open_, high_, low_, close_, volume_):
        self.table.update_one(
            {
                "_id": f"{symbol_}_{date_}",
            },
            {
                "$set": {
                    'open': open_,
                    'high': high_,
                    'low': low_,
                    'close': close_,
                    'volume': volume_
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
