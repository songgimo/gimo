import hmac
import hashlib
import requests
import time
import websocket
from decimal import Decimal
from urllib.parse import urlencode

from Util.pyinstaller_patch import debugger


class ResultObject(object):
    def __init__(self, success, data, message, time_):
        self.success = success
        self.data = data
        self.message = message
        self.time_ = time_


class TransType(object):
    SPOT = 'SPOT'
    ISOLATED = 'ISOLATED_MARGIN'


class TradeType(object):
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    BORROW = 'BORROW'
    REPAY = 'REPAY'
    NORMAL = 'NORMAL'
    BUY = 'BUY'
    SELL = 'SELL'
    
    MARGIN = 'MARGIN'
    FUTURE = 'FUTURE'


class Urls(object):
    SERVER_TIME = '/api/v1/time'
    EXCHANGE_INFO = '/api/v3/exchangeInfo'
    BALANCE = '/api/v3/account'
    TICKER = '/api/v3/ticker/price'
    
    DEPOSITS = '/sapi/v1/capital/deposit/address'
    WITHDRAW = '/wapi/v3/withdraw.html'
    
    GET_ALL_INFORMATION = '/sapi/v1/capital/config/getall'
    SNAPSHOT = '/sapi/v1/accountSnapshot'
    
    NORMAL_ORDER = '/api/v3/order'
    FUTURE_ORDER = '/dapi/v1/order'
    FUTURE_EXCHANGE = '/dapi/v1/exchangeInfo'
    
    class Margin(object):
        ORDER = '/sapi/v1/margin/order'
        BORROW = '/sapi/v1/margin/loan'
        REPAY = '/sapi/v1/margin/repay'
        ASSET = '/sapi/v1/margin/asset'
        MARGIN_PAIR = '/sapi/v1/margin/allPairs'
        ISOLATED_PAIR = '/sapi/v1/margin/isolated/allPairs'
        MARGIN_ACCOUNT = '/sapi/v1/margin/account'
        ISOLATED_ACCOUNT = '/sapi/v1/margin/isolated/account'
        MARGIN_TRANSFER = '/sapi/v1/margin/transfer'
        ISOLATED_TRANSFER = '/sapi/v1/margin/isolated/transfer'
        MARGIN_RATE = '/gateway-api/v1/friendly/isolated-margin/ladder/{}'
        MARGIN_INDEX = '/sapi/v1/margin/priceIndex'
        
        MAX_BORROWABLE = '/sapi/v1/margin/maxBorrowable'
    
    class WebSocket(object):
        GET_SPOT_LISTEN_KEY = '/api/v3/userDataStream'
        PING_SPOT_LISTEN_KEY = '/api/v3/userDataStream'
        
        GET_MARGIN_LISTEN_KEY = '/sapi/v1/userDataStream'
        PING_MARGIN_LISTEN_KEY = '/sapi/v1/userDataStream'
        
        GET_ISOLATED_LISTEN_KEY = '/sapi/v1/userDataStream/isolated'
        PING_ISOLATED_LISTEN_KEY = '/sapi/v1/userDataStream/isolated'


class BaseBinance(object):
    def __init__(self, key, secret):
        self._base_url = 'https://api.binance.com'
        self._page_endpoint = 'https://www.binance.com'
        self._future_endpoint = 'https://dapi.binance.com'
        self._exchange_info = dict()
        self.name = 'Binance'
        
        self._key = key
        self._secret = secret
        
        self._default_header = {"X-MBX-APIKEY": self._key}
    
    def _sign_generator(self, *args):
        params, *_ = args
        if params is None:
            params = dict()
        params.update({'timestamp': int(time.time() * 1000) - 5000})
        params.update({'recvWindow': 60000})

        sign = hmac.new(self._secret.encode('utf-8'),
                        urlencode(sorted(params.items())).encode('utf-8'),
                        hashlib.sha256
                        ).hexdigest()

        params.update({'signature': sign})

        return params

    def page_request(self, method, path, extra=None):
        try:
            debugger.debug('{}::: Parameters=[{}, {}], function name=[_margin_request]'.format(self.name, path, extra))
        
            if extra is None:
                extra = dict()
        
            rq = requests.get(self._page_endpoint + path)
        
            response = rq.json()
            if 'msg' in response:
                msg = '{}::: ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(self.name, response['msg'], path, extra)
                debugger.debug(msg)
                return ResultObject(False, '', msg, 1)
            else:
                return ResultObject(True, response, '', 0)
        except Exception as ex:
            msg = '{}::: ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(self.name, ex, path, extra)
            debugger.debug(msg)
            return ResultObject(False, '', msg, 1)

    def _public_api(self, path, extra=None):
        debugger.debug('{}::: Parameters=[{}, {}], function name=[_public_api]'.format(self.name, path, extra))
        if extra is None:
            extra = dict()

        try:
            rq = requests.get(self._base_url + path, params=extra)
            response = rq.json()

            if 'msg' in response:
                msg = '{}::: ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(self.name, response['msg'], path, extra)
                debugger.debug(msg)
                return ResultObject(False, '', msg, 1)
            else:
                return ResultObject(True, response, '', 0)

        except Exception as ex:
            msg = '{}::: ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(self.name, ex, path, extra)
            debugger.debug(msg)
            return ResultObject(False, '', msg, 1)

    def _private_api(self, method, path, extra=None, fail_time=1):
        debugger.debug('{}::: Parameters=[{}, {}], function name=[_private_api]'.format(self.name, path, extra))

        if extra is None:
            extra = dict()

        try:
            query = self._sign_generator(extra)
            sig = query.pop('signature')
            query = "{}&signature={}".format(urlencode(sorted(extra.items())), sig)

            if method == 'GET' or 'margin' in path:
                rq = requests.get(self._base_url + path, params=query, headers={"X-MBX-APIKEY": self._key})
            elif method == 'PUT':
                rq = requests.put(self._base_url + path, params=query, headers={"X-MBX-APIKEY": self._key})
            else:
                rq = requests.post(self._base_url + path, data=query, headers={"X-MBX-APIKEY": self._key})
            response = rq.json()

            if 'msg' in response and response['msg']:
                msg = 'ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(response['msg'], path, extra)
                debugger.debug(msg)
                return ResultObject(False, '', msg, fail_time)
            else:
                return ResultObject(True, response, '', 0)

        except Exception as ex:
            msg = '{}::: ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(self.name, ex, path, extra)
            debugger.debug(msg)
            return ResultObject(False, '', msg, fail_time)
    
    def _get_servertime(self):
        return self._public_api('GET', Urls.SERVER_TIME)

    def get_exchange_info(self):
        return self._public_api(Urls.EXCHANGE_INFO)

    def get_all_information(self):
        return self._private_api('GET', Urls.GET_ALL_INFORMATION)

    def get_snapshot(self, type_):
        return self._private_api('GET', Urls.SNAPSHOT, {'type': type_}, fail_time=10)

    def get_balance(self):
        return self._private_api('GET', Urls.BALANCE)
    
    def get_deposit_address(self, symbol, network=None):
        additional = dict()
        
        if network:
            additional = dict(network=network)
        params = {
            'coin': symbol,
            **additional
        }
        
        return self._private_api('GET', Urls.DEPOSITS, params)
    
    def get_ticker(self):
        return self._public_api(Urls.TICKER)
    
    def normal_buy(self, symbol, quantity, trade_type, price=None):
        additional = dict()
        if trade_type == TradeType.LIMIT:
            additional.update({'price': Decimal(price).quantize(Decimal(10) ** -8),
                              'timeInForce': 'GTC'})

        params = {
            'symbol': symbol,
            'type': trade_type,
            'side': 'buy',
            'quantity': Decimal(quantity).quantize(Decimal(10) ** -8),
            **additional
        }

        return self._private_api('POST', Urls.NORMAL_ORDER, params)

    def normal_sell(self, symbol, quantity, trade_type, price=None):
        additional = dict()
        if trade_type == TradeType.LIMIT:
            additional.update({'price': Decimal(price).quantize(Decimal(10) ** -8),
                              'timeInForce': 'GTC'})

        params = {
            'symbol': symbol,
            'type': trade_type,
            'side': 'sell',
            'quantity': Decimal(quantity).quantize(Decimal(10) ** -8),
            **additional
        }

        return self._private_api('POST', Urls.NORMAL_ORDER, params)

    def withdraw(self, coin, amount, to_address, payment_id=None, network=None):
        params = {
            'asset': coin,
            'address': to_address,
            'amount': '{}'.format(amount),
            'name': 'BinanceWithdraw',
        }

        if payment_id:
            tag_dic = {'addressTag': payment_id}
            params.update(tag_dic)
        if network:
            params.update({'network': network})

        return self._private_api('POST', Urls.WITHDRAW, params)


class BinanceMargin(BaseBinance):
    def __init__(self, key, secret):
        super(BinanceMargin, self).__init__(key, secret)
    
    def _future_request(self, method, path, extra=None, fail_time=1):
        debugger.debug('{}::: Parameters=[{}, {}], function name=[_future_request]'.format(self.name, path, extra))
    
        if extra is None:
            extra = dict()
    
        try:
            query = self._sign_generator(extra)
            sig = query.pop('signature')
            query = "{}&signature={}".format(urlencode(sorted(extra.items())), sig)
            if method == 'GET':
                rq = requests.get(self._future_endpoint + path, params=query, headers={"X-MBX-APIKEY": self._key})
            else:
                rq = requests.post(self._future_endpoint + path + '?' + query, headers={"X-MBX-APIKEY": self._key})

            response = rq.json()
        
            if 'msg' in response:
                msg = 'ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(response['msg'], path, extra)
                debugger.debug(msg)
                return ResultObject(False, '', msg, 1)
            else:
                return ResultObject(True, response, '', 0)
    
        except Exception as ex:
            msg = '{}::: ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(self.name, ex, path, extra)
            debugger.debug(msg)
            return ResultObject(False, '', msg, 1)

    def _margin_request(self, method, path, extra=None):
        debugger.debug('{}::: Parameters=[{}, {}], function name=[_margin_request]'.format(self.name, path, extra))
    
        if extra is None:
            extra = dict()
    
        try:
            query = self._sign_generator(extra)
            sig = query.pop('signature')
            query = "{}&signature={}".format(urlencode(sorted(extra.items())), sig)
            rq = requests.post(self._base_url + path, params=query, headers={"X-MBX-APIKEY": self._key})
            response = rq.json()
        
            if 'msg' in response:
                msg = 'ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(response['msg'], path, extra)
                debugger.debug(msg)
                return ResultObject(False, '', msg, 1)
            else:
                return ResultObject(True, response, '', 0)
    
        except Exception as ex:
            msg = '{}::: ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(self.name, ex, path, extra)
            debugger.debug(msg)
            return ResultObject(False, '', msg, 1)

    def borrow(self, asset, amount, symbol=None, is_isolated=False):
        additional = dict(symbol=symbol, isIsolated='TRUE') if is_isolated else dict()
        params = {
            'asset': asset,
            'amount': amount,
            **additional
        }
        
        return self._margin_request('POST', Urls.Margin.BORROW, params)
        
    def repay(self, asset, amount, symbol=None, is_isolated=False):
        additional = dict(symbol=symbol, isIsolated='TRUE') if is_isolated else dict()
        params = {
            'asset': asset,
            'amount': amount,
            **additional
        }

        return self._margin_request('POST', Urls.Margin.REPAY, params)
    
    def get_margin_open_order(self, id_, symbol, is_isolated=True):
        return self._private_api('GET', Urls.Margin.ORDER, {'symbol': symbol, 'orderId': id_,
                                                            'isIsolated': 'TRUE' if is_isolated else 'FALSE'})
    
    def get_future_open_order(self, id_, symbol, is_isolated=True):
        return self._private_api('GET', Urls.FUTURE_ORDER, {'symbol': symbol, 'orderId': id_,
                                                            'isIsolated': 'TRUE' if is_isolated else 'FALSE'})

    def get_all_margin_pair(self):
        return self._private_api('GET', Urls.Margin.MARGIN_PAIR)

    def get_all_isolated_pair(self):
        return self._private_api('GET', Urls.Margin.ISOLATED_PAIR)
    
    def get_margin_account(self):
        return self._private_api('GET', Urls.Margin.MARGIN_ACCOUNT)

    def get_isolated_account(self):
        return self._private_api('GET', Urls.Margin.ISOLATED_ACCOUNT)

    def get_margin_rate(self, symbol):
        return self.page_request('GET', Urls.Margin.MARGIN_RATE.format(symbol))

    def get_margin_price_index(self, symbol):
        return self._private_api('GET', Urls.Margin.MARGIN_INDEX, {'symbol': symbol})
    
    def get_margin_max_borrowable(self, asset, isolated_symbol=None):
        additional = {'isolatedSymbol': isolated_symbol} if isolated_symbol else dict()
        
        params = {
            'asset': asset,
            **additional
        }
        
        return self._private_api('GET', Urls.Margin.MAX_BORROWABLE, params)
        
    def buy(self, symbol, quantity, trade_type, price=None, is_isolated=True):
        side = 'BUY'
        
        additional = dict()
        if trade_type == TradeType.LIMIT:
            additional.update({'price': Decimal(price).quantize(Decimal(10) ** -8),
                              'timeInForce': 'GTC'})

        if is_isolated:
            additional.update(isIsolated='TRUE')

        params = {
            'symbol': symbol,
            'side': side,
            'type': trade_type,
            'quantity': Decimal(quantity).quantize(Decimal(10) ** -8),
            **additional
        }
        
        return self._margin_request('POST', Urls.Margin.ORDER, params)
        
    def sell(self, symbol, quantity, trade_type, price=None, is_isolated=True):
        side = 'SELL'
        
        additional = dict()
        if trade_type == TradeType.LIMIT:
            additional.update({'price': Decimal(price).quantize(Decimal(10) ** -8),
                              'timeInForce': 'GTC'})

        if is_isolated:
            additional.update(isIsolated='TRUE')

        params = {
            'symbol': symbol,
            'side': side,
            'type': trade_type,
            'quantity': Decimal(quantity).quantize(Decimal(10) ** -8),
            **additional
        }

        return self._margin_request('POST', Urls.Margin.ORDER, params)
    
    def _isolated_transfer(self, asset, symbol, from_, to, amount):
        params = dict(
            asset=asset,
            symbol=symbol,
            transFrom=from_,
            transTo=to,
            amount=amount
        )
        return self._margin_request('POST', Urls.Margin.ISOLATED_TRANSFER, params)
    
    def send_spot_to_isolated_wallet(self, asset, symbol, amount):
        return self._isolated_transfer(asset=asset, symbol=symbol, from_=TransType.SPOT, to=TransType.ISOLATED,
                                       amount=amount)
    
    def send_isolated_to_spot_wallet(self, asset, symbol, amount):
        return self._isolated_transfer(asset=asset, symbol=symbol, from_=TransType.ISOLATED, to=TransType.SPOT,
                                       amount=amount)

    def get_future_information(self):
        return self._future_request('GET', Urls.FUTURE_EXCHANGE)
    
    def future_buy(self, symbol, quantity, trade_type, price=None, reduce_only=False, account_object=None):
        """
            LIMIT
            MARKET
            STOP
            STOP_MARKET
            TAKE_PROFIT
            TAKE_PROFIT_MARKET
            TRAILING_STOP_MARKET
        """
        side = 'BUY'
        
        additional = dict()
        if trade_type == TradeType.LIMIT:
            additional.update({'price': Decimal(price).quantize(Decimal(10) ** -8),
                              'timeInForce': 'GTC'})
        
        if reduce_only:
            additional.update(dict(reduceOnly='true'))
        
        params = {
            'symbol': symbol,
            'side': side,
            'type': trade_type,
            'quantity': Decimal(quantity).quantize(Decimal(10) ** -8),
            **additional
        }
        
        result = self._future_request('POST', Urls.FUTURE_ORDER, params)
        
        if result.success:
            msg = '[{}]해당 계정 [{}]를 [{}]만큼 [{}]가격으로 [{}]하게 {}[buy]하였습니다.'.format(
                account_object.account, symbol, quantity, price if price else TradeType.MARKET,
                trade_type, '청산' if reduce_only else '거래'
            )
        else:
            msg = '[{}]해당 계정 [{}]를 [{}]만큼 [{}]가격으로 [{}]하게 {}[buy]하지 못했습니다.'.format(
                account_object.account, symbol, quantity, price if price else TradeType.MARKET, trade_type,
                trade_type, '청산' if reduce_only else '거래'
            )

        result.message = msg
        return result
        
    def future_sell(self, symbol, quantity, trade_type, price=None, reduce_only=False, account_object=None):
        side = 'SELL'
        
        additional = dict()
        if trade_type == TradeType.LIMIT:
            additional.update({'price': Decimal(price).quantize(Decimal(10) ** -8),
                              'timeInForce': 'GTC'})

        if reduce_only:
            additional.update(dict(reduceOnly='true'))

        params = {
            'symbol': symbol,
            'side': side,
            'type': trade_type,
            'quantity': Decimal(quantity).quantize(Decimal(10) ** -8),
            **additional
        }
        
        result = self._future_request('POST', Urls.FUTURE_ORDER, params)

        if result.success:
            msg = '[{}]해당 계정 [{}]를 [{}]만큼 [{}]가격으로 [{}]하게 {}[sell]하였습니다.'.format(
                account_object.account, symbol, quantity, price if price else TradeType.MARKET,
                trade_type, '청산' if reduce_only else '거래'
            )
        else:
            msg = '[{}]해당 계정 [{}]를 [{}]만큼 [{}]가격으로 [{}]하게 거래[sell]하지 못했습니다.'.format(
                account_object.account, symbol, quantity, price if price else TradeType.MARKET,
                trade_type, '청산' if reduce_only else '거래'
            )

        result.message = msg
        return result


class BinanceWebsocket(object):
    def __init__(self, key, secret):
        super(BinanceWebsocket, self).__init__()
        self._key, self._secret = key, secret
        self.url = 'wss://stream.binance.com:9443'
        self._base_url = 'https://api.binance.com'

        self.isolated_listen = self.IsolatedListenObject(self)
        
    def _private_api(self, method, path, extra=None, fail_time=1):
        if extra is None:
            extra = dict()

        try:
            if method == 'PUT':
                rq = requests.put(self._base_url + path, params=extra, headers={"X-MBX-APIKEY": self._key})
            else:
                rq = requests.post(self._base_url + path, data=extra, headers={"X-MBX-APIKEY": self._key})
            response = rq.json()

            if 'msg' in response and response['msg']:
                msg = 'ERROR_BODY=[{}], URL=[{}], PARAMETER=[{}]'.format(response['msg'], path, extra)
                debugger.debug(msg)
                return ResultObject(False, '', msg, fail_time)
            else:
                return ResultObject(True, response, '', 0)

        except Exception as ex:
            debugger.debug(msg)
            return ResultObject(False, '', msg, fail_time)

    class IsolatedListenObject(object):
        def __init__(self, parent):
            self.parent = parent
            self._created_listen_key_time = None
        
        def get_listen_url(self, isolated_listen_key):
            return self.parent.url + '/stream?streams=' + isolated_listen_key
        
        def refresh(self, symbol):
            res_object = self.parent._private_api('POST', Urls.WebSocket.GET_ISOLATED_LISTEN_KEY,
                                                  {'symbol': symbol})
            return res_object
        
        def ping(self, isolated_listen_key, symbol):
            return self.parent._private_api('PUT', Urls.WebSocket.PING_MARGIN_LISTEN_KEY,
                                            {'listenKey': isolated_listen_key, 'symbol': symbol})
