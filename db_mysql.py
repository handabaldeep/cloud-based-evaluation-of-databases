import mysql.connector as mysql


class MySqlDB:
    def __init__(self, host, port, user, password, database):
        self.conn = mysql.connect(
                        host=host,
                        port=port,
                        user=user,
                        password=password,
                        database=database
                    )
        self.cur = self.conn.cursor()
        self.db_name = database
        self.table_name = "ohlc"

    def _execute_query(self, query_):
        self.cur.execute(query_)
        self.conn.commit()

    def _execute_query_with_results(self, query_):
        self.cur.execute(query_)
        return self.cur.fetchall()

    def create_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} (" \
                "symbol VARCHAR (255), " \
                "trading_date DATE, " \
                "open_price REAL, " \
                "high_price REAL, " \
                "low_price REAL, " \
                "close_price REAL, " \
                "volume INTEGER, " \
                "PRIMARY KEY (symbol, trading_date)" \
                ")"
        self._execute_query(query)

    def insert(self, symbol_, date_, open_, high_, low_, close_, volume_):
        query = f"INSERT INTO {self.table_name} " \
                f"(symbol, trading_date, open_price, high_price, low_price, close_price, volume) " \
                f"VALUES ('{symbol_}', '{date_}', {open_}, {high_}, {low_}, {close_}, {volume_})"
        self._execute_query(query)

    def read(self, symbol_, date_):
        query = f"SELECT * FROM {self.table_name} " \
                f"WHERE symbol='{symbol_}' AND trading_date>='{date_}'"
        return len(self._execute_query_with_results(query))

    def update(self, symbol_, date_, open_, high_, low_, close_, volume_):
        query = f"UPDATE {self.table_name} " \
                f"SET open_price={open_}, high_price={high_}, low_price={low_}, close_price={close_}, " \
                f"volume={volume_} " \
                f"WHERE symbol='{symbol_}' AND trading_date='{date_}'"
        self._execute_query(query)

    def _cleanup(self):
        query = f"TRUNCATE {self.table_name}"
        self._execute_query(query)

    def get_size(self):
        query = f"SELECT table_name as 'Table', " \
                f"round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB` " \
                f"FROM information_schema.TABLES " \
                f"WHERE table_schema = '{self.db_name}' " \
                f"AND table_name = '{self.table_name}'"
        return self._execute_query_with_results(query)[0][1]

    def close(self):
        self._cleanup()
        self.cur.close()
        self.conn.close()
