from DiffTrader.server.models import ProfitSettingQueries, ExpectedProfitQueries, SlippageDataQueries
from flask_restful import Resource
from flask import jsonify, request


class ProfitSettingTable(Resource):
    def get(self):
        args = request.args
        user_id = args.get('user_id', None)

        result = ProfitSettingQueries.get_profit_setting_table(user_id)

        return result

    def put(self):
        args = request.args
        user_id = args.get('user_id', None)

        if user_id is None:
            return
        min_profit_percent = args.get('min_profit_percent')
        min_profit_btc = args.get('min_profit_btc')
        auto_withdrawal = args.get('auto_withdrawal')

        value_list = [user_id, min_profit_percent, min_profit_btc, auto_withdrawal]

        if self._value_validator(value_list):
            ProfitSettingQueries.insert_profit_setting_table(value_list)
            return True
        else:
            return False

    def _value_validator(self, list_):
        for each in list_:
            if not each:
                return False
        else:
            return True


class ExpectedProfitTable(Resource):
    def get(self):
        """
            trade_object.trade_date,
            trade_object.symbol,
            trade_object.primary_exchange,
            trade_object.secondary_exchange,
            trade_object.profit_btc,
            trade_object.profit_percent,
        """
        args = request.args
        user_id = args.get('user_id', None)

        date_from, date_to = args.get('date_from'), args.get('date_to')

        result = ExpectedProfitQueries.get_expected_profit_table(user_id, date_from, date_to)

        return result

    def put(self):
        args = request.args
        user_id = args.get('user_id', None)

        trade_date = args.get('trade_date')
        symbol = args.get('symbol')
        primary_exchange = args.get('primary_exchange')
        secondary_exchange = args.get('secondary_exchange')
        profit_btc = args.get('profit_btc')
        profit_percent = args.get('profit_percent')

        value_list = [
            trade_date, symbol, primary_exchange, secondary_exchange, profit_btc, profit_percent
        ]

        ExpectedProfitQueries.put_expected_profit_table(user_id, value_list)


class SlippageDataTable(Resource):
    def get(self):
        args = request.args
        user_id = args.get('user_id', None)
        if user_id:
            result = SlippageDataQueries.get_slippage_data(user_id)
            return result

    def put(self):
        args = request.args
        user_id = args.get('user_id', None)
        
        coin = args.get('coin')
        market = args.get('market')
        exchange = args.get('exchange')
        orderbooks = args.get('orderbooks')
        tradings = args.get('tradings')
        trading_type = args.get('trading_type')
        orderbook_timestamp = args.get('orderbook_timestamp')
        trading_timestamp = args.get('trading_timestamp')
        
        value_list = [
            user_id, coin, market, exchange, orderbooks, tradings,
            trading_type, orderbook_timestamp, trading_timestamp
        ]
        
        SlippageDataQueries.put_slippage_data(user_id, value_list)
