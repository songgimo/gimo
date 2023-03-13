import threading
import queue
import numpy as np
import re
import datetime

from PyQt5.QtCore import QObject
from Util.pyinstaller_patch import debugger
from datetime import datetime

GET_STOCK_INFO_TX_CODE = 'opt10001'
GET_ALL_PRICE_TX_CODE = 'opt10004'
GET_CURRENT_PRICE_TX_CODE = 'opt10007'
GET_ACCOUNT_INFO_TX_CODE = 'OPW00004'
GET_MARGIN_DATA_TX_CODE = 'opw00011'
GET_BULK_FILLED_DATA_TX_CODE = 'opt10055'
GET_INVEST_FUND_VOLUME_TX_CODE = 'opt10059'

GET_DAILY_CANDLE_TX_CODE = 'opt10005'
GET_DAILY_CHART_TX_CODE = 'opt10081'

USE_REPEAT = 2
UNUSED_REPEAT = 0
DELISTING_VIEW = '0'
EXCEPT_DELISTING_VIEW = '1'

DEFAULT_INPUT_PASSWORD_TYPE = '00'

# 매수 & 매도 코드
NEW_BUY_ORDER = 1
NEW_SELL_ORDER = 2
CANCEL_BUY = 3
CANCEL_SELL = 4
CHANGE_BUY = 5
CHANGE_SELL = 6

# 구매 시 지정가 & 시장가 변수
LIMIT_PRICE = '00'
MARKET_PRICE = '03'

# Chejan 관련 FID list
GET_STOCK_CODE = '9001'
GET_STOCK_FILLED_PRICE_CODE = '910'
GET_STOCK_FILLED_QTY_CODE = '911'
GET_ORDER_NUMBER = '9203'

# 일반적 에러 코드 ( 더 있음 )
ERROR_CODE = {
    '0': '정상처리',
    '-200': '시세과부하',
    '-301': '계좌번호 없음',
    '-308': '주문전송 과부하',
}

# SetRealReg관련
ONLY_LAST_REGISTRY = '0'
ADD_REGISTRY = '1'

CURRENT_PRICE_CODE = '10'

FILLED_DATE = '20'
TRADE_AMOUNT = '15'

# 임의의 screen number
REAL_CURRENT_PRICE_SCREEN_NUM = '1001'
REAL_ORDERBOOK_SCREEN_NUM = '1002'
CONDITION_SCREEN_NUM = '1003'
REAL_STOCK_FILLED_SCREEN_NUM = '1004'

# 증거금율 관련 screen number
MARGIN_SCREEN_NUM = '0011'

# 종목별투자자기관요청 opt10059 투신 관련
INVEST_FUND_QUANTITY = '2'
INVEST_FUND_BUYING = '1'
INVEST_FUND_NET_BUYING = '0'
INVEST_FUND_SINGLE_UNIT = '1'


# TODO
"""
매매취소
실시간현재가
시가
trade 시 발생하는 주문번호 가져오기
"""


def get_step(market, price):
    if price < 1000:
        return 1
    elif price < 5000:
        return 5
    elif price < 10000:
        return 10
    elif price < 50000:
        return 50
    elif market == 'kosdaq' or price < 100000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000


class KiwoomAPIModule(QObject):
    def __init__(self, controller):
        super().__init__()
        self._controller = controller
        self._lock = threading.RLock()
        self._orderbook_lock = threading.Lock()
        self._service_q = dict()
        self._real_service_q = dict()
        self._order_history = dict()
        self._meet_real_conditions = list()
        self.is_connected = False
        self._first_bulk_filled_flag = True
        self.condition_list = list()
        
        self.set_auto_screen = True
        self._screen_number_counter = 1
        self._auto_screen_number = 1001
    
    def __call__(self, *args, **kwargs):
        print('call')
    
    def _auto_screen_setter(self):
        if self._screen_number_counter >= 100:
            self._screen_number_counter = 1
            self._auto_screen_number += 1
        else:
            self._screen_number_counter += 1

    def _set_value(self, name, value):
        # we need to set value using this function before request data.
        return self._controller.dynamicCall('SetInputValue(QString, QString)', name, value)

    def _request_common_data(self, name, tx_code, screen_num, repeat=UNUSED_REPEAT):
        # request to KiwoomAPI and return result.
        return self._controller.dynamicCall('commRqData(QString, QString, int, QString)',
                                            [name, tx_code, repeat, screen_num])

    def _get_repeat_count(self, tx_code, rq_name):
        return self._controller.dynamicCall('GetRepeatCnt(QString, QString)', [tx_code, rq_name])

    def _send_order(self, rq_name, scn_no, account, order_type, stock_code, qty, price, trade_type, ogn_order_no=''):
        # 매매 주문을 하는 함수
        '''
              SendOrder(
              BSTR sRQName, // 사용자 구분명
              BSTR sScreenNo, // 화면번호
              BSTR sAccNo,  // 계좌번호 10자리
              LONG nOrderType,  // 주문유형 1:신규매수, 2:신규매도 3:매수취소, 4:매도취소, 5:매수정정, 6:매도정정
              BSTR sCode, // 종목코드
              LONG nQty,  // 주문수량
              LONG nPrice, // 주문가격
              BSTR sHogaGb,   // 거래구분(혹은 호가구분)은 아래 참고
              BSTR sOrgOrderNo  // 원주문번호입니다. 신규주문에는 공백, 정정(취소)주문할 원주문번호를 입력합니다.
              )
        '''

        self._service_q[rq_name] = queue.Queue()

        res_code = self._controller.dynamicCall(
            'SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)',
            [rq_name, scn_no, account, order_type, stock_code, qty, price, trade_type, ogn_order_no]
        )

        if str(res_code) == '0':
            trading_type = rq_name.split('_')[-1]
            order_number = self._service_q[rq_name].get(True, timeout=10)
            if 'cancel' in rq_name:
                self._order_history.pop(ogn_order_no, None)
            elif order_number:
                if ogn_order_no in self._order_history:
                    filled = self._order_history[ogn_order_no]['filled']
                    self._order_history.pop(ogn_order_no, None)
                else:
                    filled = 0
                filled_price = 0
                self._order_history[order_number] = dict(
                    order_id=order_number,
                    price=price,
                    amount=qty,
                    trade_type=trading_type,
                    filled=filled,
                    filled_price=filled_price,
                    stock_code=stock_code
                )

            return order_number

        else:
            return ERROR_CODE.get(str(res_code))

    def get_kospi_stock_codes(self):
        ret = self._controller.dynamicCall("GetCodeListByMarket(QString)", ["0"])
        return ret.split(';')

    def get_kosdaq_stock_codes(self):
        ret = self._controller.dynamicCall("GetCodeListByMarket(QString)", ["10"])
        return ret.split(';')

    def get_stock_codes(self):
        kospi = self.get_kospi_stock_codes()
        kosdaq = self.get_kosdaq_stock_codes()
        return dict(kospi=kospi, kosdaq=kosdaq)

    def get_all_stock_korean_name(self, scn, code_list, callback_q):
        code_list_len = str(len(code_list))
        str_code_list = ';'.join(code_list)
        rq_name = '{}_{}'.format(str_code_list, '대량종목명')
        if '대량종목명' not in self._service_q:
            self._service_q['대량종목명'] = callback_q

        self._controller.dynamicCall("CommKwRqData(QString, int, int, int, QString, QString)",
                                     [str_code_list, "0", code_list_len, '0', rq_name, scn])

    def get_stock_korean_name(self, code, callback_q):
        # Return korean name base on stock code
        return self.get_stock_information(code, item_name='종목명')

    def get_current_price(self, code):
        raw_data = self.get_stock_information(code, item_name='현재가')
        data = re.sub(r'[^\d]', '', raw_data)
        return int(data)

    def get_highest_price(self, code):
        raw_data = self.get_stock_information(code, item_name='상한가')
        data = re.sub(r'[^\d]', '', raw_data)
        return int(data)

    def get_opening_price(self, code):
        raw_data = self.get_stock_information(code, item_name='시가')
        data = re.sub(r'[^\d]', '', raw_data)
        return int(data)

    def register_condition_list(self, condition_list):
        self.condition_list = condition_list

    def login_connect_check(self):
        # Check login connect is successful or not, return 1 if login success else 0
        # 로그인이 정상적으로 되었는지에 대한 Check, 로그인 되어있는 경우 1, 아닌 경우 0을 반환한다.
        return self._controller.dynamicCall("GetConnectState()")

    def _processing_orderbook_set(self, raw_data):
        regex_processed = [re.sub(r'[^\d]', '', data) for data in raw_data[0][:61]]
        ask_orderbook = regex_processed[1:31]
        bid_orderbook = regex_processed[31:61]

        ask_data = np.array(ask_orderbook)
        bid_data = np.array(bid_orderbook)

        shape = (10, 3)

        ask_order = ask_data.reshape(shape)
        bid_order = bid_data.reshape(shape)

        ask_order_dict = dict(ask_order[:, [2, 1]])
        bid_order_dict = dict(bid_order[:, [0, 1]])

        return ask_order_dict, bid_order_dict

    def get_orderbook_set(self, code):
        return self._real_service_q.get('orderbook', {}).get(code, None)

    def get_current_price_set(self, code):
        return self._real_service_q.get('current_price', {}).get(code, None)

    def get_real_filled_data(self, code):
        return self._real_service_q.get('stock_filled', dict()).get(code, None)

    def get_order_history(self, order_number):
        if not order_number:
            return None
        return self._order_history.get(order_number, None)

    def get_stock_information(self, code, item_name):
        screen_number = code
        rq_name = '{}_{}'.format(code, item_name)
        self._set_value('종목코드', code)
        self._service_q[rq_name] = queue.Queue()
        self._request_common_data(rq_name, GET_STOCK_INFO_TX_CODE, screen_number)

        return self._service_q[rq_name].get(True, timeout=10)

    def buy_stock(self, account, stock_code, qty, price=0, trade_type=LIMIT_PRICE):
        # 매수함수, 신규 매수는 1번임
        with self._lock:
            screen_number = stock_code

            rq_name = '{}_buy'.format(stock_code)

            return self._send_order(rq_name, screen_number, account, NEW_BUY_ORDER, stock_code, qty, price, trade_type)

    def sell_stock(self, account, stock_code, qty, price=0, trade_type=LIMIT_PRICE):
        # 매도함수, 신규매도는 2번임
        with self._lock:
            screen_number = stock_code
            rq_name = '{}_sell'.format(stock_code)

            return self._send_order(rq_name, screen_number, account, NEW_SELL_ORDER, stock_code, qty, price, trade_type)

    def cancel_buy_stock(self, account, stock_code, qty, order_number, price=0, trade_type=LIMIT_PRICE):
        # 매수취소 = 3번
        # 매매 취소 시에는 취소할 주문 번호를 반드시 입력 받야아 함.
        with self._lock:
            screen_number = stock_code
            rq_name = '{}_cancel_buy'.format(stock_code)

            return self._send_order(rq_name, screen_number, account, CANCEL_BUY, stock_code, qty, price,
                                    trade_type, order_number)

    def cancel_sell_stock(self, account, stock_code, qty, order_number, price=0, trade_type=LIMIT_PRICE):
        # 매도취소 = 4번
        # 매매 취소 시에는 취소할 주문 번호를 반드시 입력 받야아 함.
        with self._lock:
            screen_number = stock_code
            rq_name = '{}_cancel_sell'.format(stock_code)

            return self._send_order(rq_name, screen_number, account, CANCEL_SELL, stock_code, qty, price,
                                    trade_type, order_number)

    def correct_buy_stock(self, account, stock_code, qty, order_number, price=0, trade_type=LIMIT_PRICE):
        with self._lock:
            screen_number = stock_code
            rq_name = '{}_correct_buy'.format(stock_code)

            return self._send_order(rq_name, screen_number, account, CHANGE_BUY, stock_code, qty, price,
                                    trade_type, order_number)

    def correct_sell_stock(self, account, stock_code, qty, order_number, price=0, trade_type=LIMIT_PRICE):
        with self._lock:
            screen_number = stock_code
            rq_name = '{}_correct_sell'.format(stock_code)

            return self._send_order(rq_name, screen_number, account, CHANGE_SELL, stock_code, qty, price,
                                    trade_type, order_number)

    def get_common_real_data(self, stock_code, real_type):
        return self._controller.dynamicCall('GetCommRealData(QString, int)',
                                            [stock_code, real_type])

    def get_common_data_with_repeat(self, tx_code, rq_name, index, item_name):
        return self._controller.dynamicCall('GetCommData(QString, QString, int, QString)',
                                            [tx_code, rq_name, index, item_name]).replace(' ', '')

    def get_chejan_data(self, fid):
        return self._controller.dynamicCall('GetChejanData(int)', [int(fid)])
    
    def get_common_data(self, tx_code, rc_name, index, item_name, verbose=False):
        # return tx_data for getting signal
        if verbose:
            debugger.debug('{} {} {}'.format(tx_code, rc_name, item_name))
        return self._controller.dynamicCall('GetCommData(QString, QString, int, QString)',
                                            [tx_code, rc_name, index, item_name]).replace(' ', '')

    def get_common_data_ex(self, tx_code, rc_name):
        return self._controller.dynamicCall('GetCommDataEx(QString, QString)', [tx_code, rc_name])

    def _get_stock_quantity(self, raw_data):
        qty_dict = dict()
        for data in raw_data:
            code, _, quantity, *_ = data[0]
            quantity = re.sub(r'[^\d]', '', quantity)
            qty_dict.update({code: int(quantity)})
        return qty_dict

    def get_bulk_filled_data(self, stock_code, callback_queue, date_index=1):
        """
            stock_code: 주식코드
            date_index: 1: 오늘 2: 어제
        """
        stock_code = stock_code[0]
        debugger.debug('get_bulk_filled_data = args=[{}, {}, {}]'.format(stock_code, callback_queue, date_index))
        self._set_value('종목코드', stock_code)
        self._set_value('당일전일', date_index)

        screen_number = stock_code
        rq_name = '당일전일체결대량요청'

        self._service_q[rq_name] = callback_queue
        if self._first_bulk_filled_flag:
            self._request_common_data(rq_name, GET_BULK_FILLED_DATA_TX_CODE, screen_number[:4])
            self._first_bulk_filled_flag = False
        else:
            self._request_common_data(rq_name, GET_BULK_FILLED_DATA_TX_CODE, screen_number[:4], USE_REPEAT)

    def get_daily_candle_within_month(self, stock_code, callback_queue):
        debugger.debug('get_daily_candle_within_month = args=[{}]'.format(stock_code))
        self._set_value('종목코드', stock_code)
    
        screen_number = stock_code
        rq_name = '주식일주월시분요청'
        self._service_q[rq_name] = callback_queue
        self._set_value('종목코드', stock_code)
    
        self._request_common_data(rq_name, GET_DAILY_CANDLE_TX_CODE, screen_number[:4])

    def get_all_daily_candle(self, stock_code, date, callback_queue, repeat):
        debugger.debug('get_all_daily_candle = args=[{}]'.format(stock_code))
        self._set_value('종목코드', stock_code)

        screen_number = stock_code
        rq_name = '주식일봉차트조회요청'
        self._service_q[rq_name] = callback_queue
        self._set_value('종목코드', stock_code)
        self._set_value('기준일자', date)
        self._set_value('수정주가구분', 1)
        
        if self.set_auto_screen:
            self._auto_screen_setter()
        
        self._request_common_data(rq_name, GET_DAILY_CHART_TX_CODE, self._auto_screen_number, repeat)

    def get_stock_amount(self, account, stock):
        self._set_value('계좌번호', account)
        self._set_value('비밀번호', '')
        self._set_value('상장폐지조회구분', EXCEPT_DELISTING_VIEW)
        self._set_value('비밀번호입력매체구분', DEFAULT_INPUT_PASSWORD_TYPE)

        screen_number = account
        rq_name = '계좌평가현황요청'

        self._service_q[rq_name] = queue.Queue()
        self._request_common_data(rq_name, GET_ACCOUNT_INFO_TX_CODE, screen_number[:4])

        data = self._service_q[rq_name].get(True, timeout=10)
        if stock == '예수금':
            return data['cash_balance']
        else:
            for code, amount in data['stock_balance'].items():
                if stock in code:
                    return amount
        return None

    def get_margin_information(self, tx_code, rc_name):
        stock_margin_rate = int(self.get_common_data(tx_code, rc_name, UNUSED_REPEAT, '종목증거금율').replace('%', ''))
        account_margin_rate = int(self.get_common_data(tx_code, rc_name, UNUSED_REPEAT, '계좌증거금율').replace('%', ''))

        higher_rate = stock_margin_rate if stock_margin_rate >= account_margin_rate else account_margin_rate

        available_margin_order_price = self.get_common_data(tx_code, rc_name, UNUSED_REPEAT,
                                                            '증거금{}주문가능금액'.format(higher_rate))
        get_margin_yesterday_reusable = self.get_common_data(tx_code, rc_name, UNUSED_REPEAT,
                                                             '증거금{}전일재사용금액'.format(higher_rate))
        get_margin_today_reusable = self.get_common_data(tx_code, rc_name, UNUSED_REPEAT,
                                                         '증거금{}금일재사용금액'.format(higher_rate))

        available_margin_order_price, get_margin_yesterday_reusable, get_margin_today_reusable = list(
            map(int, (available_margin_order_price, get_margin_yesterday_reusable, get_margin_today_reusable)))
        available = available_margin_order_price + get_margin_yesterday_reusable + get_margin_today_reusable

        return available

    def get_available_amount(self, account, stock_code, stock_bid_price):
        self._set_value('계좌번호', account)
        self._set_value('비밀번호', '')
        self._set_value('비밀번호입력매체구분', DEFAULT_INPUT_PASSWORD_TYPE)
        self._set_value('종목번호', stock_code)
        self._set_value('매수가격', stock_bid_price)
        rq_name = '증거금율별주문가능수량조회요청'

        self._service_q[rq_name] = queue.Queue()
        self._request_common_data(rq_name, GET_MARGIN_DATA_TX_CODE, MARGIN_SCREEN_NUM)

        return self._service_q[rq_name].get(True, timeout=10)

    def get_invest_fund_volume(self, date, stock_code):
        """
                date: 일자 = YYYYMMDD (20160101 연도4자리, 월 2자리, 일 2자리 형식)
                stock_code: 종목코드 = 전문 조회할 종목코드
                amount_index: 금액수량구분 = 1:금액, 2:수량
                sale_index: 매매구분 = 0:순매수, 1:매수, 2:매도
                unit_index: 단위구분 = 1000:천주, 1:단주
        """
        self._set_value('일자', date)
        self._set_value('종목코드', stock_code)
        self._set_value('금액수량구분', INVEST_FUND_QUANTITY)
        self._set_value('매매구분', INVEST_FUND_NET_BUYING)
        self._set_value('단위구분', INVEST_FUND_SINGLE_UNIT)
        rq_name = '종목별투자자기관별요청'
        screen_num = stock_code

        self._service_q[rq_name] = queue.Queue()
        self._request_common_data(rq_name, GET_INVEST_FUND_VOLUME_TX_CODE, screen_num)

        return self._service_q[rq_name].get(True, timeout=10)

    def _set_real_reg(self, screen_number, code_list, fid_list, real_type):
        self._controller.dynamicCall('SetRealReg(QString, QString, QString, QString)',
                                     [screen_number, code_list, fid_list, real_type])

    def _remove_real_reg(self, screen_number, stock_code):
        # 종목 별 실시간 해제 함수.
        self._controller.dynamicCall('SetRealRemove(QString, QString)',
                                     [screen_number, stock_code])

    def registry_real_current_price_data(self, stock_code_list):
        for code in stock_code_list:
            self._real_service_q.setdefault('current_price', {code: list()})

        stock_code_list = ';'.join(stock_code_list)
        return self._set_real_reg(screen_number=REAL_CURRENT_PRICE_SCREEN_NUM,
                                  code_list=stock_code_list,
                                  fid_list=CURRENT_PRICE_CODE,
                                  real_type=ONLY_LAST_REGISTRY)

    def add_real_current_price_data(self, stock_code):
        return self._set_real_reg(screen_number=REAL_CURRENT_PRICE_SCREEN_NUM,
                                  code_list=stock_code,
                                  fid_list=CURRENT_PRICE_CODE,
                                  real_type=ADD_REGISTRY)

    def remove_real_current_price_data(self, stock_code):
        self._real_service_q['current_price'].pop(stock_code)

        return self._remove_real_reg(screen_number=REAL_CURRENT_PRICE_SCREEN_NUM,
                                     stock_code=stock_code)

    def registry_real_orderbook_data(self, stock_code_list, fid_list):
        for code in stock_code_list:
            self._real_service_q.setdefault('orderbook', {code: list()})
        debugger.debug(self._real_service_q)

        stock_code_list = ';'.join(stock_code_list)
        fid_list = ';'.join(fid_list)
        return self._set_real_reg(screen_number=REAL_ORDERBOOK_SCREEN_NUM,
                                  code_list=stock_code_list,
                                  fid_list=fid_list,
                                  real_type=ONLY_LAST_REGISTRY)

    def add_real_orderbook_data(self, stock_code):
        return self._set_real_reg(screen_number=REAL_ORDERBOOK_SCREEN_NUM,
                                  code_list=stock_code,
                                  fid_list=CURRENT_PRICE_CODE,
                                  real_type=ADD_REGISTRY)

    def remove_real_orderbook_data(self, stock_code):
        self._real_service_q.get('orderbook', {}).pop(stock_code, None)

        return self._remove_real_reg(screen_number=REAL_ORDERBOOK_SCREEN_NUM,
                                     stock_code=stock_code)

    def registry_real_stock_filled_data(self, stock_code_list, callback_q, fid_list=None):
        if fid_list is None:
            fid_list = [FILLED_DATE, TRADE_AMOUNT]
        for code in stock_code_list:
            if 'stock_filled' not in self._real_service_q:
                self._real_service_q.setdefault('stock_filled', {code: callback_q})
            else:
                self._real_service_q.get('stock_filled').update({code: callback_q})

        debugger.debug(self._real_service_q)

        stock_code_list = ';'.join(stock_code_list)
        fid_list = ';'.join(fid_list)
        return self._set_real_reg(screen_number=REAL_STOCK_FILLED_SCREEN_NUM,
                                  code_list=stock_code_list,
                                  fid_list=fid_list,
                                  real_type=ONLY_LAST_REGISTRY)

    def add_real_stock_filled_data(self, stock_code, fid_list=None):
        if fid_list is None:
            fid_list = [FILLED_DATE, TRADE_AMOUNT]

        return self._set_real_reg(screen_number=REAL_ORDERBOOK_SCREEN_NUM,
                                  code_list=stock_code,
                                  fid_list=fid_list,
                                  real_type=ADD_REGISTRY)

    def remove_real_stock_filled_data(self, stock_code_list):
        for stock_code in stock_code_list:
            self._real_service_q.get('stock_filled', {}).pop(stock_code, None)
            self._remove_real_reg(screen_number=REAL_STOCK_FILLED_SCREEN_NUM, stock_code=stock_code)

    def apply_conditions(self):
        return self._controller.dynamicCall('GetConditionLoad()')

    def get_condition(self, code):
        return code in self._meet_real_conditions

    def get_conditions(self):
        return self._meet_real_conditions

    def get_account_list(self):
        return self._controller.dynamicCall('GetLoginInfo("ACCLIST")').split(';')

    def connect_status_receiver(self, code):
        if code == 0:
            debugger.debug('kiwoom_login has successfully connected.')
            self.is_connected = True
        else:
            debugger.debug('kiwoom_login has failed')
            self.is_connected = False

    def receive_tx_data(self, *arg):
        debugger.debug(arg)
        try:
            scn_no, rq_name, tx_code, rc_name, repeat, d_len, err_code, msg, sp_msg = arg
        except Exception as ex:
            debugger.exception("FATAL")
            return
        try:
            if '계좌평가현황요청' in rq_name:
                data = {}
                cash_balance = self.get_common_data(tx_code, rc_name, UNUSED_REPEAT, '예수금')
                data['cash_balance'] = re.sub(r'[^\d]', '', cash_balance)
                data['stock_balance'] = dict()
                for i in range(self._get_repeat_count(tx_code, rq_name)):
                    code = self.get_common_data_with_repeat(tx_code, rq_name, i, '종목코드')
                    current_stock = self.get_common_data_with_repeat(tx_code, rq_name, i, '보유수량')
                    data['stock_balance'][code] = current_stock
                self._service_q[rq_name].put(data)
            elif '종목명' in rq_name or '상한가' in rq_name or '시가' in rq_name or '현재가' in rq_name:
                code_set, item_name = rq_name.split('_')
                if item_name == '대량종목명':
                    # 대량종목명인 경우: get_all_stock_korean_name 를 사용할 때
                    dic_ = dict()
                    code_list = code_set.split(';')
                    for i in range(self._get_repeat_count(tx_code, rq_name)):
                        res = self.get_common_data_with_repeat(tx_code, rq_name, i, '종목명')
                        dic_.update({code_list[i]: res})
                    self._service_q[item_name].put(dic_)
                else:
                    result = self.get_common_data(tx_code, rc_name, UNUSED_REPEAT, item_name)
                    self._service_q[rq_name].put(result)

            elif '증거금' in rq_name:
                result = self.get_margin_information(tx_code, rc_name)
                self._service_q[rq_name].put(result)

            elif 'buy' in rq_name or 'sell' in rq_name:
                result = self.get_common_data(tx_code, rc_name, UNUSED_REPEAT, '주문번호')
                self._service_q[rq_name].put(result)

            elif '주식일주월시' in rq_name:
                # 최대 30일 내 주식 일봉값들이 들어옴.
                data_set = list()
                for i in range(self._get_repeat_count(tx_code, rq_name)):
                    date = self.get_common_data_with_repeat(tx_code, rq_name, i, '날짜')
                    last_price = self.get_common_data_with_repeat(tx_code, rq_name, i, '종가')
                    date_timestamp = datetime.datetime.strptime(date, '%Y%m%d').timestamp() * 1000
                    data_set.append([date_timestamp, last_price])
                else:
                    self._service_q[rq_name].put(data_set)
            
            elif '주식일봉차트조회요청' in rq_name:
                data_set = list()
                for i in range(self._get_repeat_count(tx_code, rq_name)):
                    date = self.get_common_data_with_repeat(tx_code, rq_name, i, '일자')
                    close = self.get_common_data_with_repeat(tx_code, rq_name, i, '현재가')
                    open_ = self.get_common_data_with_repeat(tx_code, rq_name, i, '시가')
                    high = self.get_common_data_with_repeat(tx_code, rq_name, i, '고가')
                    low = self.get_common_data_with_repeat(tx_code, rq_name, i, '저가')
                    date_timestamp = datetime.datetime.strptime(date, '%Y%m%d').timestamp() * 1000
                    data_set.append([date_timestamp, open_, high, low, close])
                else:
                    if int(repeat) == USE_REPEAT:
                        self._service_q[rq_name].put((True, data_set))
                
                    else:
                        self._service_q[rq_name].put((False, data_set))
                
            elif '당일전일' in rq_name:
                for i in range(self._get_repeat_count(tx_code, rq_name)):
                    filled_time = self.get_common_data_with_repeat(tx_code, rq_name, i, '체결시간')
                    price = self.get_common_data_with_repeat(tx_code, rq_name, i, '체결가')
                    fluctuation_per = self.get_common_data_with_repeat(tx_code, rq_name, i, '등락률')
                    filled_amount = self.get_common_data_with_repeat(tx_code, rq_name, i, '체결량')
                    acu_amount = self.get_common_data_with_repeat(tx_code, rq_name, i, '누적거래량')
                    self._service_q[rq_name].put(('tx_data', dict(
                        filled_time=filled_time,
                        price=price,
                        filled_amount=filled_amount,
                        fluctuation_per=fluctuation_per,
                        acu_amount=acu_amount)))
                else:
                    if int(repeat) == USE_REPEAT:
                        self._service_q[rq_name].put(('tx_data', dict()))

            elif '종목별투자자기관별요청' in rq_name:
                dic_ = dict()
                for i in range(self._get_repeat_count(tx_code, rq_name)):
                    invest_volume = int(self.get_common_data(tx_code, rc_name, i, '투신'))
                    date = self.get_common_data(tx_code, rc_name, i, '일자')
                    dt = datetime.strptime(date, "%Y%m%d").date()
                    dic_.setdefault(dt, invest_volume)
                self._service_q[rq_name].put(dic_)

            else:
                self._service_q[rq_name].put(arg)
        except Exception as ex:
            debugger.exception("FATAL")
            return

    def receive_real_tx_data(self, *args):
        try:
            code, real_type, real_data = args
            if real_type == '주식호가잔량' and 'orderbook' in self._real_service_q:
                with self._lock:
                    real_data = real_data.split('\t')
                    ask_orderbook = {
                        real_data[i]: real_data[i + 1]
                        for i in range(55, 0, -6)
                    }
                    bid_orderbook = {
                        real_data[i]: real_data[i + 1]
                        for i in range(4, 59, 6)
                    }
                    with self._orderbook_lock:
                        self._real_service_q['orderbook'][code] = [ask_orderbook, bid_orderbook]
                    debugger.debug('{} -> {}'.format(code, self._real_service_q['orderbook'][code]))
            elif real_type in ['주식시세', '주식체결', '주식예상체결'] and 'current_price' in self._real_service_q:
                with self._lock:
                    # res = self.get_common_real_data(code, real_type)
                    # self._real_service_q['current_price'][code] = res
                    self._real_service_q['current_price'][code] = abs(int(real_data.split('\t')[1]))

            elif real_type == '주식체결' and 'stock_filled' in self._real_service_q:
                with self._lock:
                    # stock code with leading 'A' can be given
                    code = code[-6:]
                    if code in self._real_service_q['stock_filled']:
                        self._real_service_q['stock_filled'][code].put(('real_tx_data', real_data))

        except Exception as ex:
            debugger.exception('FATAL')
            return

    def receive_chejan_data(self, *args):
        try:
            debugger.debug("chejan: {}".format(args))
            code, real_type, real_data = args

            fid_list = real_data.split(';')
            if GET_STOCK_CODE in fid_list and GET_STOCK_FILLED_QTY_CODE in fid_list and GET_STOCK_FILLED_PRICE_CODE in fid_list and GET_ORDER_NUMBER in fid_list:
                order_number = self.get_chejan_data(GET_ORDER_NUMBER)
                stock_code = self.get_chejan_data(GET_STOCK_CODE)
                filled = self.get_chejan_data(GET_STOCK_FILLED_QTY_CODE)
                filled_price = self.get_chejan_data(GET_STOCK_FILLED_PRICE_CODE)

                if order_number not in self._order_history:
                    debugger.debug('Order number is not in order history')
                    return

                if filled_price and filled:
                    filled_price = float(filled_price)
                    filled = int(filled)

                    self._order_history[order_number]['filled_price'] = filled_price
                    self._order_history[order_number]['filled'] = filled

                if stock_code.startswith('A'):
                    stock_code = stock_code[1:]

                debugger.debug('code: {} filled: {} filled price: {}'.format(stock_code, filled, filled_price))

                if self._order_history.get(stock_code, None):
                    self._order_history[stock_code].update(dict(filled=filled))

        except Exception as ex:
            debugger.exception('FATAL')
            return

    def receive_msg(self, *args):
        debugger.debug("msg: {}".format(args))

    def receive_condition_ver(self, *args, **kwargs):
        try:
            debugger.debug("condition ver: {}".format(args))
            conditions = self._controller.dynamicCall('GetConditionNameList()')
            debugger.debug("conditions: {}".format(conditions))
            if not conditions:
                return

            conditions = conditions.strip(';').split(';')
            for condition in conditions:
                condition_index, condition_name = condition.split('^')
                if condition_name == 'VI발동API':
                    self._controller.dynamicCall('SendCondition(QString, QString, int, int)',
                                                 [CONDITION_SCREEN_NUM, condition_name, int(condition_index), 1])
                    debugger.debug('VI발동API 등록완료.')
                elif condition_name in self.condition_list:
                    self._controller.dynamicCall('SendCondition(QString, QString, int, int)',
                                                 [CONDITION_SCREEN_NUM, condition_name, int(condition_index), 1])
                    debugger.debug('{} 등록완료.'.format(condition_name))
        except Exception as ex:
            debugger.exception('FATAL')
            return

    def receive_real_condition(self, *args):
        """
        Kiwoom Receive Realtime Condition Result(stock list) Callback, 조건검색 실시간 편입, 이탈 종목을 받을 시점을 알려준다.
        condi_name(조건식)으로 부터 검출된 종목이 실시간으로 들어옴.
        update_type으로 편입된 종목인지, 이탈된 종목인지 구분한다.
        * 조건식 검증할때, 어떤 종목이 검출된 시간을 본 함수내에서 구현해야 함
        :param code str: 종목코드
        :param event_type str: 편입("I"), 이탈("D")
        :param condi_name str: 조건식명
        :param condi_index str: 조건식명 인덱스
        :return: 없음
        """
        try:
            debugger.debug("real condition: {}".format(args))
            code, event_type, condi_name, condi_index = args
            if event_type == "I":
                self._meet_real_conditions.append(code)
            elif event_type == "D" and code in self._meet_real_conditions:
                self._meet_real_conditions.remove(code)
            debugger.debug('met: {}'.format(self._meet_real_conditions))
        except Exception as ex:
            debugger.exception('FATAL')
            return
