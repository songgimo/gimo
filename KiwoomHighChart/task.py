from StockApis.kiwoom import KiwoomAPIModule
from PyQt5.QAxContainer import QAxWidget
from PyQt5 import (
    QtCore
)
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import pyqtSignal

from multiprocessing import Queue

import queue
import random

from Util.pyinstaller_patch import *
from KiwoomHighChart.query import GetQueries, PutQueries, TableQueries

"""
    checklist
    1. DB확인, 언제까지 일봉이 저장되어 있는지 확인 필요
        1-1. 저장되어 있는 경우 가장 최근 일봉의 date를 가져와서 처리
        1-2. 저장되어 있지 않는 경우 처음부터 repeat돌려서 다 가져오기
        1-3. 장 종료 이후 돌려서 오늘날짜까지 값을 저장하거나, 장 도중에는 전날까지만 가져옴
    2. repeat여부를 통해 처음 일봉이 쌓이기 시작한 날짜를 파악할 수 있는지 확인 필요함.


"""


class KiwoomHighChart(QtCore.QThread):
    def __init__(self, res_queue, remains):
        """
            QAx, open_api를 작동시키기 위한 main thread
        """
        debugger.debug('KiwoomHighChart:::Start')
        
        super(KiwoomHighChart, self).__init__()
        self.result_queue = res_queue
        self.remain_codes = remains
        self.set_events()
        
    def set_events(self):
        debugger.debug('KiwoomHighChart:::set_events')
        self.kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.kiwoom.dynamicCall("CommConnect()")
        self.kiwoom_api = KiwoomAPIModule(self.kiwoom)
        
        self.kiwoom.OnReceiveTrData.connect(self.kiwoom_api.receive_tx_data)
        self.kiwoom.OnReceiveChejanData.connect(self.kiwoom_api.receive_chejan_data)
        self.kiwoom.OnEventConnect.connect(self.kiwoom_api.connect_status_receiver)
        
        self.kiwoom_thread = KiwoomThread(self.kiwoom, self.kiwoom_api, self.result_queue, self.remain_codes)
        
        self.kiwoom_thread.start()


class KiwoomThread(QtCore.QThread):
    terminate_signal = pyqtSignal()
    
    def __init__(self, kiwoom, kiwoom_api, res_queue, remains):
        debugger.debug('KiwoomThread:::Start')
        
        super(KiwoomThread, self).__init__()
        self.kiwoom = kiwoom
        self.kiwoom_api = kiwoom_api
        
        self._queue = Queue()
        self.result_queue = res_queue
        self.remain_codes = remains
        
        self._total_queue = Queue()
        self.daemon = True

    def run(self):
        try:
            for _ in range(300):
                if self.kiwoom_api.is_connected:
                    break
                time.sleep(1)
            TableQueries.set_stock_info()
            TableQueries.set_daily_info()
            if not self.remain_codes:
                # 첫 시작, 혹은 정상적인 시작의 경우 remain_codes는 none이다.
                TableQueries.set_stock_info()
                TableQueries.set_daily_info()
                total_codes, _ = self.insert_all_stocks_code_name()
                self.remain_codes = total_codes

            res = self.get_all_stocks_daily_candle(self.remain_codes)

            if res is True:
                # 정상적으로 종료가 된 경우.
                # except처리되어 종료된 경우에는 별개의 except signal로 처리한다.
                self.result_queue.put((True, str()))
            else:
                self.result_queue.put((False, res))

        except Exception as ex:
            print(ex)
    
    def get_index_stock_names(self, scn, codes):
        indexes = list()
        for i in range(0, len(codes), 100):
            indexes.append(i)
        end = len(codes)
        indexes.reverse()
        totals = dict()
        for index in indexes:
            code_list = codes[index:end]
            self.kiwoom_api.get_all_stock_korean_name(scn, code_list, self._queue)
            res = self._queue.get(timeout=20)
            totals.update(res)
            end = index
            time.sleep(0.2)
        return totals
    
    def get_stocks_daily_candle_not_entered(self, data_set, date):
        for n, data in enumerate(data_set):
            if date in data:
                # 오늘 제외, 전날부터 미입력 값 넣어야 함.
                # 최대 600일전까지 나오는데, 그정도로 값이 갱신이 안되진 않을 것이라고 판단함.
                return data_set[:n]
    
    def insert_all_stocks_code_name(self):
        codes = self.kiwoom_api.get_stock_codes()
        kospi_codes = self.get_index_stock_names('9542', codes['kospi'])
        kosdaq_codes = self.get_index_stock_names('9543', codes['kosdaq'])
        total_code_name_set = dict(**kospi_codes, **kosdaq_codes)
        total_codes = codes['kospi'] + codes['kosdaq']
        try:
            PutQueries.code_and_name(total_code_name_set)
        except Exception as ex:
            debugger.debug('fail to update stock codes&names, [{}]'.format(ex))
        
        return total_codes, total_code_name_set

    def get_all_stocks_daily_candle(self, total_codes):
        # 전날기준 600일 이전 값
        latest_date = (datetime.datetime.now() - datetime.timedelta(days=1))
        for n, code in enumerate(total_codes):
            if not code:
                # 코드값이 비어있는 경우가 있음.
                continue
            
            repeat = 0
            input_date = GetQueries.is_exist_table_by_stock_code(code)  # timestamp
            input_date = list(input_date)
        
            get_total_data_thread = threading.Thread(target=self.get_all_daily_thread,
                                                     args=[code, latest_date, input_date, repeat],
                                                     daemon=True)
            get_total_data_thread.start()
            try:
                total_data_set = self._total_queue.get(timeout=10)
            except queue.Empty:
                debugger.debug('daily candle get failed from [{}]'.format(code))
                after = total_codes[n:]
                return after
        
            if total_data_set:
                try:
                    PutQueries.daily_candle(code, total_data_set)
                except Exception as ex:
                    debugger.info('fail to put the daily_candle from = [{}]'.format(code))
            time.sleep(random.randrange(200, 4000) / 1000)
        return True

    def get_all_daily_thread(self, *args):
        code, latest_date, input_date, repeat = args
        total_data_set = list()
        while True:
            self.kiwoom_api.get_all_daily_candle(code, latest_date.strftime('%Y%m%d'), self._queue,
                                                 repeat=repeat)
            is_repeat, data_set = self._queue.get(timeout=10)
            repeat = 2
        
            if input_date:
                # 기존 데이터가 있는 경우, 1회 데이터 추가
                not_entered = self.get_stocks_daily_candle_not_entered(data_set, input_date[0][0])
                if not_entered:
                    total_data_set += not_entered
                break
            else:
                # 반복시 마지막 날짜를 기준으로 가져오므로
                if is_repeat is False:
                    # 반복상태가 아닌 경우 break해서 수집종료
                    total_data_set += data_set
                    break
                else:
                    total_data_set += data_set[:-1]
                time.sleep(random.randrange(200, 4000) / 1000)
        self._total_queue.put(total_data_set)
        return


def processor(res_queue, remains):
    app = QApplication(sys.argv)
    kh = KiwoomHighChart(res_queue, remains)
    app.exec_()


if __name__ == '__main__':
    multiprocessing.freeze_support()
    remain_codes = None
    result_queue = Queue()
    while True:
        kp = multiprocessing.Process(name='KiwoomProcessor', target=processor, args=(result_queue, remain_codes), daemon=True)
        kp.start()
        
        success, remain_codes = result_queue.get()
        
        if success is False:
            kp.terminate()
            time.sleep(5)
            continue
        else:
            kp.terminate()
            time.sleep(60 * 60 * 24)
