from DiffTrader.trading.settings import SAI_URL, PROFIT_SAI_URL, \
    SAVE_DATA_URL, LOAD_DATA_URL, MethodType

import requests
import copy
import time
from datetime import datetime


def get_expected_profit(user_id, data_receive_queue, after_process=None):
    """
        Get expected_profit from saiblockchain api server.
    """
    def callback(result):
        if result:
            result = copy.deepcopy(result)
            for date_ in result:
                profit_date = datetime.fromtimestamp(date_[0]).strftime(
                    '%Y{} %m{} %d{} %H{} %M{}').format('년', '월', '일', '시', '분')
                date_[0] = profit_date

        return result if result else list()

    now_date = time.time()
    yesterday = now_date - 24 * 60 * 60

    information_dict = {
        'parameter': {'user_id': user_id, 'from': yesterday, 'to': now_date},
        'after_process': after_process,
        'callback': callback
    }

    data_receive_queue.put((PROFIT_SAI_URL, MethodType.GET, information_dict))

    return list()

def send_expected_profit(profit_object, data_receive_queue, after_process=None):
    information_dict = {'parameter': profit_object.information}

    data_receive_queue.put((SAI_URL, MethodType.POST, information_dict))


def send_slippage_data(user_id, data_dict, data_receive_queue, after_process=None):
    """
        data_dict:
            coin, market, exchange, amount, orderbooks, tradings, orderbooks_timestamp, trading_timestamp
    """
    information_dict = {
        'parameter': {'user_id': user_id, **data_dict}
    }

    data_receive_queue.put((SAI_URL, MethodType.POST, information_dict))


def save_total_data_to_database(id_key, min_profit_percent, min_profit_btc, is_withdraw, data_receive_queue,
                                after_process=None):
    dic = dict()
    row = [id_key, min_profit_percent, min_profit_btc, is_withdraw]
    for num, each in enumerate(['id_key', 'min_profit_percent', 'min_profit_btc', 'is_withdraw']):
        dic.setdefault(each, row[num])

    information_dict = {'parameter': dic,
                        'callback': after_process}

    data_receive_queue.put((SAVE_DATA_URL, MethodType.GET, information_dict))


def load_total_data_to_database(id_key, data_receive_queue, after_process=None):
    def callback(raw_result):
        if not raw_result:
            return dict()
        
        result = raw_result

        min_profit_percent = result.get('min_profit_percent')
        min_profit_btc = result.get('min_profit_btc')
        is_withdraw = True if result.get('is_withdraw') else False
        return dict(
            min_profit_percent=min_profit_percent,
            min_profit_btc=min_profit_btc,
            auto_withdrawal=is_withdraw
        )

    information_dict = {
        'callback': callback,
        'after_process': after_process,
        'parameter': dict(id_key=id_key)
    }
    data_receive_queue.put((LOAD_DATA_URL, MethodType.GET, information_dict))

