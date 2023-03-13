import threading
import queue

from PyQt5.QtCore import QObject
from Util.pyinstaller_patch import debugger

DEPOSIT_RATES = {
    'A': 0.15,
    'B': 0.2,
    'C': 0.25,
    'D': 1
}


class DataInfo(object):
    """
        데이터 조회용 변수 class
    """
    ACCOUNTS = 'AccountList'
    
    REMAINS_INFO = 'SABA200QB'
    TOTAL_ASSET_INFO = 'SABA655Q1'
    DEPOSIT_RATE_STATUS = 'stock_mst'


class IndiAPI(QObject):
    """
        신한i Indi API 모듈
    """
    
    def __init__(self, controller):
        super().__init__()
        
        self._lock = threading.RLock()
        
        self._controller = controller
        self._orderbook_lock = threading.Lock()
        
        self._deposit_rates = dict()
        
        self._service_q = dict()
        self._real_service_q = dict()
        
        self._rqid_set = dict()
    
    def get_account_list(self):
        return self._base_dynamic_call_set(DataInfo.ACCOUNTS)
    
    def get_deposit_rate(self, stock_code):
        if not self._deposit_rates:
            self._deposit_rates = self._base_dynamic_call_set(DataInfo.DEPOSIT_RATE_STATUS)
        return self._deposit_rates.get(stock_code, 1)
    
    def get_deposit(self, deposit_number, deposit_password):
        """
            deposit_number: str
            deposit_password: str
        """
        res = self._deposit_contents(DataInfo.TOTAL_ASSET_INFO, deposit_number, deposit_password)
        return res
    
    def get_stock_amount(self, stock_code, deposit_number, deposit_password):
        res = self._deposit_contents(DataInfo.REMAINS_INFO, deposit_number, deposit_password)
        return res[stock_code]
    
    def receive_data(self, rqid):
        with self._lock:
            tr_name = self._rqid_set[rqid]
        res = dict()
        debugger.debug('receive_data, DataType [{}], rqid [{}]'.format(tr_name, rqid))
        if tr_name == DataInfo.ACCOUNTS:
            cnt = self._controller.dynamicCall("GetMultiRowCount()")
            info = list()
            for i in range(cnt):
                code = self._controller.dynamicCall("GetMultiData(int, int)", i, 0)
                name = self._controller.dynamicCall("GetMultiData(int, int)", i, 1)
                info.append(dict(account_number=code, account_name=name))
            res.update(dict(account_info=info))
        
        elif tr_name == DataInfo.TOTAL_ASSET_INFO:
            deposit_index = 17
            deposit = self._controller.dynamicCall("GetSingleData(int)", deposit_index)
            res.update(dict(deposit=int(deposit or 0)))
        
        elif tr_name == DataInfo.DEPOSIT_RATE_STATUS:
            cnt = self._controller.dynamicCall("GetMultiRowCount()")
            stock_code_index = 1
            rate_index = 11
            for i in range(cnt):
                stock_code = self._controller.dynamicCall("GetMultiData(int, int)", i, stock_code_index)
                margin_rate = self._controller.dynamicCall("GetMultiData(int, int)", i, rate_index)
                res.update({stock_code: DEPOSIT_RATES.get(margin_rate, 0)})
        elif tr_name == DataInfo.REMAINS_INFO:
            cnt = self._controller.dynamicCall("GetMultiRowCount()")
            rows = ['stock_code', 'stock_name', 'amount']
            info = list()
            for i in range(cnt):
                each_dict = dict()
                for num, row in enumerate(rows):
                    data = self._controller.dynamicCall("GetMultiData(int, int)", i, num)
                    if row == 'amount':
                        data = int(data)
                    elif row == 'stock_code':
                        data = data[1:]
                    each_dict.update({row: data})
                info.append(each_dict)
            res.update(dict(remains=info))
        
        self._service_q[tr_name].put(res)
    
    def receive_message(self, msg_id):
        debugger.debug('RecevieMessage, [{}]'.format(msg_id))
        
        try:
            error_msg = self._controller.dynamicCall("GetErrorMessage()")
            state = self._controller.dynamicCall("GetErrorState()")
            
            debugger.debug('receive error msg [{}], state [{}]'.format(error_msg, state))
        except Exception as ex:
            debugger.debug('fail to recevied, [{}]'.format(ex))
    
    def _request_rqid(self, data_type):
        with self._lock:
            rqid = self._controller.dynamicCall("RequestData()")
            
            debugger.debug('request_rqid, rqId [{}]'.format(rqid))
            
            self._rqid_set.setdefault(rqid, data_type)
            self._service_q[data_type] = queue.Queue()
    
    def _base_dynamic_call_set(self, data_type):
        debugger.debug('base_dynamic_call_set, DataType [{}]'.format(data_type))
        self._controller.dynamicCall("SetQueryName(QString)", data_type)
        self._request_rqid(data_type)
        
        return self._service_q[data_type].get(True, timeout=10)
    
    def _deposit_contents(self, data_type, number, password):
        """
            계좌번호, 상품구분과 비밀번호를 사용하는 함수들이 사용함.
            number: 계좌 넘버
            password: 계좌 비밀번호
        """
        debugger.debug('deposit_contents, DataType [{}], DepositNumber [{}]'.format(data_type, number, password))
        
        self._controller.dynamicCall("SetQueryName(QString)", data_type)
        self._controller.dynamicCall("SetSingleData(int, QString)", 0, number)
        self._controller.dynamicCall("SetSingleData(int, QString)", 1, "01")
        self._controller.dynamicCall("SetSingleData(int, QString)", 2, password)
        
        self._request_rqid(data_type)
        
        return self._service_q[data_type].get(True, timeout=10)

