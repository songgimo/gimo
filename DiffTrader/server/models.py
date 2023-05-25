from DiffTrader.server.util import execute_db_many, execute_db




class ProfitSettingQueries(object):
    @staticmethod
    def create_min_profit_data_table():
        query = """
            CREATE TABLE IF NOT EXISTS profit_setting_table(
                user_id INT PRIMARY KEY,
                min_profit_percent float NOT NULL,
                min_profit_btc float NOT NULL,
                auto_withdrawal boolean NOT NULL
            )
        """

        return execute_db(query)

    @staticmethod
    def get_profit_setting_table(user_id):
        query = """
            SELECT user_id
            FROM profit_setting_table
            WHERE user_id = %s
        """
        return execute_db(query, value=user_id)

    @staticmethod
    def insert_profit_setting_table(value_list):
        query = """
            INSERT INTO profit_setting_table(user_id, min_profit_percent, min_profit_btc, auto_withdrawal)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            min_profit_percent = VALUES(min_profit_percent),
            min_profit_btc = VALUES(min_profit_btc),
            auto_withdrawal = VALUES(auto_withdrawal)
        """
        return execute_db(query, value=value_list)


class ExpectedProfitQueries(object):
    @staticmethod
    def create_expected_profit_table():
        query = """
            CREATE TABLE IF NOT EXISTS expected_profit_table(
                user_id INT,
                trade_date DATE,
                primary_exchange CHAR(16),
                secondary_exchange CHAR(16),
                profit_btc DECIMAL(4, 8),
                profit_perecent FLOAT,
            )
        """

        return execute_db(query)

    @staticmethod
    def get_expected_profit_table(user_id, date_from, date_to):
        query = """
        SELECT trade_date, symbol, primary_exchange, secondary_exchange, profit_btc, profit_percent
        FROM expected_porift_table
        WHERE user_id = %s AND (trade_date BETWEEN date_from AND date_to)
        """

        return execute_db(query, value=[user_id, date_from, date_to])

    @staticmethod
    def put_expected_profit_table(user_id, value_list):
        query = """
        INSERT INTO expected_profit_table(user_id, trade_date, symbol, 
        primary_exchange, secondary_exchange, profit_btc, profit_percent)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        return execute_db(query, value=[user_id, *value_list])


class SlippageDataQueries(object):
    @staticmethod
    def get_slippage_data(user_id):
        query = """
        SELECT user_id, coin, market, exchange, orderbooks, tradings, trading_type, orderbook_timestamp, trading_timestamp
        FROM expected_porift_table
        WHERE user_id = %s
        """

        return execute_db(query, value=[user_id])
    
    @staticmethod
    def put_slippage_data(user_id, value_list):
        query = """
        INSERT INTO slippage_data_table(user_id, coin, market, exchange, orderbooks, tradings,
        trading_type, orderbook_timestamp, trading_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        return execute_db(query, value=[user_id, *value_list])
