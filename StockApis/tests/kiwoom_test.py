import queue
import sys

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QPushButton
from PyQt5 import QtCore

from StockApis.kiwoom import KiwwomAPIModule, FILLED_DATE, TRADE_AMOUNT


class TestKiwoomApi(QMainWindow):
    def __init__(self):
        super(TestKiwoomApi, self).__init__()
        self._command_q = queue.Queue()
        
        self._kiwoom_thread = TestKiwoomApiThread(self._command_q)
        self._kiwoom_thread.start()
        self.test_filled_event_btn()

    def test_filled_event_btn(self):
        self.filled_event_btn = QPushButton("realtime filled checker", self)
        self.filled_event_btn.setGeometry(100, 50, 100, 50)
        self.filled_event_btn.clicked.connect(self.test_filled_event)
    
    def test_filled_event(self):
        self._command_q.put('test_filled')
    
    
class TestKiwoomApiThread(QtCore.QThread):
    def __init__(self, command_q):
        super(TestKiwoomApiThread, self).__init__()
        self.kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self._command_q = command_q
        
        self.kiwoom_module = KiwwomAPIModule(self.kiwoom)
        
        self.kiwoom_api = KiwwomAPIModule(self.kiwoom)
        self.connect_events()
        
        self._code_list = [FILLED_DATE, TRADE_AMOUNT]
        self._stock_code_list = ['005930']
        
    def connect_events(self):
        self.kiwoom.OnReceiveTrData.connect(self.kiwoom_api.receive_tx_data)
        self.kiwoom.OnReceiveChejanData.connect(self.kiwoom_api.receive_chejan_data)
    
        self.kiwoom.OnReceiveRealData.connect(self.kiwoom_api.receive_real_tx_data)
        self.kiwoom.OnReceiveMsg.connect(self.kiwoom_api.receive_msg)
        self.kiwoom.OnReceiveConditionVer.connect(self.kiwoom_api.receive_condition_ver)
        self.kiwoom.OnReceiveRealCondition.connect(self.kiwoom_api.receive_real_condition)

    def run(self):
        while True:
            try:
                data = self._command_q.get(timeout=10)
                if data == 'test_filled':
                    self.kiwoom_module.registry_real_stock_filled_data(self._stock_code_list, self._code_list)
                    for code in self._code_list:
                        res = self.kiwoom_module.get_real_filled_data(code)
                        print(res)
            except Exception as ex:
                print(ex)
                
                
if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = TestKiwoomApi()
    win.show()
    app.exec_()
