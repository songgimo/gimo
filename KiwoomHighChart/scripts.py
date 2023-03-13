from KiwoomHighChart.util import execute_db, execute_db_many
from KiwoomHighChart.config import IndicatorDict

"""
    특정 데이터에 int 넣어주기,
    
"""

def script():
    all_query = """
        SELECT stock_code, indicator_name
        from stock_indicators
    """
    
    res = execute_db(all_query)
    
    l_r = list(res)
    
    for each in l_r:
        code, indi_name = each
        index = IndicatorDict.SET.get(indi_name, IndicatorDict.OTHER)
        query = """
            UPDATE stock_indicators SET order_index = %s
            WHERE stock_code = %s and indicator_name = %s
        """
        res = execute_db(query, (index, code, indi_name))

    
if __name__ == '__main__':
    script()
