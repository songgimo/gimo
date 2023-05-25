from DiffTrader.settings import DEBUG
from Util.pyinstaller_patch import debugger
from DiffTrader.trading.settings import SAI_URL, PROFIT_SAI_URL, \
    SAVE_DATA_URL, LOAD_DATA_URL, MethodType

from DiffTrader.trading.mockup import profit_table_mock, profit_setting_mock

import threading
import requests


class SenderThread(threading.Thread):
    def __init__(self, data_receive_queue):
        super(SenderThread, self).__init__()
        self._data_receive_queue = data_receive_queue
        self.daemon = True

    def run(self):
        while True:
            try:
                url, method, information_dict = self._data_receive_queue.get()

                parameter = information_dict.get('parameter', dict())
                after_process = information_dict.get('after_process', None)
                callback = information_dict.get('callback', None)
                callback_kwargs = information_dict.get('callback_kwargs', dict())
                
                if not DEBUG:
                    if method == MethodType.GET:
                        rq = requests.get(url, json=parameter)
                    elif method == MethodType.POST:
                        rq = requests.post(url, data=parameter)
                    else:
                        continue
    
                    result = rq.json()
                else:
                    if url == PROFIT_SAI_URL:
                        result = profit_table_mock()
                    elif url == LOAD_DATA_URL:
                        result = profit_setting_mock()
                    else:
                        result = dict()
                if callback:
                    callback_result = callback(result) if not callback_kwargs \
                        else callback(**callback_kwargs)
                    if after_process:
                        after_process(callback_result)
            except Exception as ex:
                debugger.debug(ex)
