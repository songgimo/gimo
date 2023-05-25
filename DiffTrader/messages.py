"""
Logging 관련 message 집합
"""

import logging
import os, sys
import datetime


# JSON, pickle 등으로 저장?
"""
    Logs 정책
    debug들은 내부에서 다 처리하고, 윗단 값들 (trade_thread.py) 등은 debug 최소화.
    대신 info들 노출시켜 주는 것에 중점을 둬야함.
"""


class Logs(object):
    def __init__(self, signal):
        self.signal = signal

    def send(self, message):
        self.signal.emit(message, logging.INFO)

    def send_error(self, message):
        self.signal.emit(message, logging.ERROR)


class Messages(object):
    """
        from_exchange: BTC를 매도하는 거래 exchange, => primary_exchange로도 표현됨.
        to_exchange: ALT 매도 후 BTC를 매수하는 거래 exchange => secondary_exchange로도 표현됨.
    """
    class Init(object):
        """
            프로그램 시작 이후부터 event가 loop돌면서 값을 찾기 전까지 message 집합
        """
        START = '자동 차익매매 감지를 시작합니다.'
        MIN_PROFIT = '최소 %: {min_profit}%'
        MIN_BTC = '최소 BTC: {min_btc}'
        WRONG_INPUT = '잘못된 값이 설정되어 있습니다. 설정 값을 확인해 주세요.'
        AUTO = '자동 출금 여부: {auto_withdrawal}'

        GET_WITHDRAWAL_INFO = '출금 정보를 가져오는 중입니다.'
        FAIL_WITHDRAWAL_INFO = '출금 정보를 가져오지 못했습니다. 정보 확인 후 다시 시작해 주세요.'
        SUCCESS_WITHDRAWAL_INFO = '출금 정보를 가져왔습니다.'

    class Trade(object):
        """
            evt가 loop 돌기 시작하는 시점부터 xx전 까지의 message 집합
        """
        SUCCESS_FEE_INFO = '수수료 조회에 성공했습니다.'
        NO_AVAILABLE = '거래 가능한 코인이 없습니다. 잔고를 확인해 주세요.'
        NO_BALANCE_BTC = 'BTC 잔고가 없습니다. 잔고를 확인해 주세요.'
        NO_PROFIT = '만족하는 조건 값을 찾지 못했습니다. 조건을 재검색 합니다.'
        NO_MIN_BTC = '최고 이익이 사용자 지정 BTC 보다 작아 거래하지 않습니다'
        
        FAIL = '거래에 실패했습니다. 처음부터 다시 시도합니다.'
        SUCCESS = '차익 거래에 성공했습니다.'

        MIN_PROFIT_ERROR = '예상 차익 %는 실수여야만 합니다.'
        
        START_TRADE = '최대 이윤 계산 결과가 설정한 BTC보다 높습니다. 거래를 시작합니다.'
        
        NO_BALANCE_ALT = '{exchange}: {alt} 잔고가 없습니다.'
        
        EXCEPT_PROFIT = '{from_exchange} -> {to_exchange}: {currency}, 예상 차익: {profit_per}'
        TRADABLE = '{from_exchange}: {alt}, {alt_amount} -> {to_exchange}: 거래 가능한 btc: {tradable_btc}'
        BTC_PROFIT = '{from_exchange} -> {to_exchange}, alt: {alt}, 수익: {btc_profit} BTC ({btc_profit_per}%)'
        
        NO_ADDRESS = '{to_exchange}: {alt}주소가 없습니다. 아래 안내대로 수동이체가 필요합니다.'
        NO_BTC_ADDRESS = '{from_exchange}: BTC주소가 없습니다. 아래 안내대로 수동이체가 필요합니다.'
        
        ALT_WITHDRAW = '{from_exchange} -> {to_exchange}로 {alt}를 {unit}개수 만큼 이동합니다.'
        BTC_WITHDRAW = '{to_exchange} -> {from_exchange}로 BTC를 {unit}개수 만큼 이동합니다.'
        
        FAIL_WITHDRAWAL = '{from_exchange} -> {to_exchange}로 {alt}를 이체하는데 실패했습니다.'
        FAIL_BTC_WITHDRAWAL = '{to_exchange} -> {from_exchange}로 BTC를 이체하는데 실패했습니다.'
        ERROR_CONTENTS = '거래에 실패했습니다. 에러 내용은 다음과 같습니다. [{}]'
        
        REQUEST_MANUAL_STOP = '에러가 계속되면 수동정지를 해주세요.'
        MANUAL_STOP = '수동정지 되었습니다. 아래 안내대로 수동 이체를 부탁드립니다.'
        COMPLETE_MANUAL = '거래가 완료되었습니다. 수동이체 후 다시 시작해주세요.'
        
    class Balance(object):
        CURRENT = '{exchange}: 잔고 {balance}'
    
    class Debug(object):
        TRADABLE = '거래 가능한 코인 종류: {}'
        ASK_BID = '{currency}의 {from_exchange}의 매도가 {from_asks} ' \
                  '{to_exchange}의 매수가{to_bids}'
        
        TRADABLE_BTC = '거래 가능한 BTC 수: {tradable_btc}'
        TRADABLE_ASK_BID = '거래 가능한 매수/매도 오더북: {from_exchange}: {from_orderbook}' \
                           '{to_exchange}: {to_orderbook}'
        
        BUY_ALT = '{from_exchange}: BTC를 통해 {alt}를 매수 하였습니다.'
        SELL_ALT = '{to_exchange}: {alt}를 매도 하였습니다.'

        BUY_BTC = '{to_exchange}: BTC를 매수 하였습니다.'

    class Error(object):
        EXCEPTION = '프로그램에 예기치 못한 문제가 발생하였습니다. 로그를 개발자에게 즉시 보내주세요.'
        FATAL = 'FATAL, TradeThread'


class QMessageBoxMessage(object):
    class Title(object):
        LOGIN_FAILED = '로그인 실패'
        SAVE_RESULT = '저장 결과'
        FAIL_LOAD = '로딩 오류'
        EXCHANGE_SETTING_ERROR = '거래소 설정 오류'
        UNEXPECTED_ERROR = '예기치 못한 오류'

    class Content(object):
        EMPTY_ID = '아이디가 빈 값입니다.'
        EMPTY_PASSWORD = '비밀번호가 빈 값입니다.'
        
        WRONG_ID = '아이디가 없거나, 잘못된 패스워드입니다.'
        EXPIRED_ID = '기간이 만료된 ID입니다.\n관리자에게 문의하세요.'
        
        SERVER_IS_CLOSED = '서버가 닫혀 있습니다.'
        
        SAVE_SUCCESS = '저장에 성공했습니다.'
        SAVE_SUCCESS_TO_SERVER = '서버로의 데이터 저장이 성공했습니다.'
        SAVE_FAIL_TO_SERVER = '서버로의 데이터 저장에 실패했습니다.'
        SAVE_FAIL = '저장에 실패했습니다.'
        WRONG_SECRET_KEY = '암호화키가 다릅니다. 세팅 파일을 초기화 하시겠습니까?'
        CANNOT_BE_SAME_EXCHANGE = '거래소 1과 거래소 2가 동일한 값이 될 수 없습니다.'
        SET_MINIMUM_TWO_EXCHANGE = '최소 거래소가 2개 이상 세팅되어 있어야 합니다.'
        REQUIRE_EXCHANGE_SETTING = '거래소 1과 거래소 2의 세팅 값이 정상적으로 입력되지 않았습니다.'
        SEND_TO_DEVELOPER = '개발자에게 debugger.log를 보내주세요.'
        WRONG_PROFIT_SETTING = '수익 설정 값이 정상적으로 입력되지 않았습니다.'
        WRONG_PROFIT_PERCENT = '최소 수익 %가 정상적으로 입력되지 않았습니다.'
        WRONG_PROFIT_BTC = '최소 수익 BTC가 정상적으로 입력되지 않았습니다.'
        WRONG_KEY_SECRET = 'API KEY나 API SECRET이 정상적으로 입력되지 않았습니다.'
