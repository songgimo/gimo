from KiwoomHighChart.util import execute_db, execute_db_many


class TableQueries(object):
    @staticmethod
    def set_indicator_table():
        query = """
            CREATE TABLE IF NOT EXISTS stock_indicators (
                stock_code varchar(16) NOT NULL,
                indicator_name varchar(32) NOT NULL,
                date int,
                value varchar(32),
                order_index int,
                FOREIGN KEY (stock_code) REFERENCES stock_info(stock_code),
                CONSTRAINT const UNIQUE (stock_code, indicator_name, date)
            )
        """
        return execute_db(query)
    
    @staticmethod
    def set_daily_info():
        query = """
            CREATE TABLE IF NOT EXISTS daily_info (
                stock_code varchar(16) NOT NULL,
                candle_date bigint NOT NULL,
                open int,
                high int,
                low int,
                close int,
                FOREIGN KEY (stock_code) REFERENCES stock_info(stock_code),
                CONSTRAINT const UNIQUE (stock_code, candle_date)
            )
        """
    
        return execute_db(query)
    
    @staticmethod
    def set_stock_info():
        query = """
            CREATE TABLE IF NOT EXISTS stock_info (
                stock_code varchar(16) NOT NULL PRIMARY KEY,
                kor_name varchar(32) NOT NULL
            )
        """
    
        return execute_db(query)


class GetQueries(object):
    @staticmethod
    def daily_candle_by_stock_kor(stock_kor):
        query = """
            SELECT candle_date, open, high, low, close
            FROM daily_info
            JOIN stock_info ON stock_info.stock_code = daily_info.stock_code
            WHERE stock_info.kor_name = %s
            ORDER BY candle_date ASC
        """
        
        return execute_db(query, value=stock_kor)
    
    @staticmethod
    def stock_indicator_by_stock_kor(stock_kor):
        query = """
            SELECT indicator_name, date, value
            FROM stock_indicators
            JOIN stock_info ON stock_info.stock_code = stock_indicators.stock_code
            WHERE stock_info.kor_name = %s
            ORDER BY order_index ASC
        """
    
        return execute_db(query, value=stock_kor)
    
    @staticmethod
    def is_exist_table_by_stock_code(stock_kor):
        query = """
            SELECT candle_date
            FROM daily_info
            WHERE stock_code = %s
            ORDER BY candle_date DESC 
            LIMIT 1
        """
    
        res = execute_db(query, value=stock_kor)
        return res
    
    @staticmethod
    def is_exist_indicator_by_stock_code(stock_kor):
        query = """
            SELECT stock_code, indicator_name, date, value, order_index
            FROM stock_indicators
            WHERE stock_code = %s
            ORDER BY order_index DESC
            LIMIT 1
        """
    
        res = execute_db(query, value=stock_kor)
        return res


class PutQueries(object):
    @staticmethod
    def indicator(value_list, stock_kor):
        query = """
            INSERT INTO stock_indicators(stock_code, indicator_name, date, value, order_index)
            VALUES ((SELECT stock_code FROM stock_info WHERE kor_name = %s), %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            date = VALUES(date),
            value = VALUES(value)

        """

        return execute_db_many(query, value_list, stock_kor)
    
    @staticmethod
    def daily_candle(stock_code, value_list):
        query = """
            INSERT IGNORE INTO daily_info(stock_code, candle_date, open, high, low, close)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        return execute_db_many(query, value_list, stock_code)
    
    @staticmethod
    def code_and_name(total_codes):
        list_ = list()
        for code in total_codes:
            list_.append([code, total_codes[code]])
    
        query = """
            INSERT IGNORE INTO stock_info(stock_code, kor_name)
            VALUES (%s, %s)
        """
    
        return execute_db_many(query, list_)


class DeleteQueries(object):
    @staticmethod
    def indicator(stock_kor):
        query = """
            DELETE stock_indicators
            FROM stock_indicators
            LEFT JOIN stock_info ON stock_info.stock_code = stock_indicators.stock_code
            WHERE stock_info.kor_name = %s
        """
        
        return execute_db(query, stock_kor)