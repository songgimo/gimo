import unittest
from unittest.mock import MagicMock, Mock


def profit_table_mock():
    """
    trade_date,
    symbol,
    primary_exchange,
    secondary_exchange,
    profit_btc,
    profit_percent
    """
    return [[1684985593, 'BTC_XRP', 'binance', 'bithumb', 0.001, 0.1],
            [1684975593, 'BTC_ETH', 'upbit', 'bithumb', 0.005, 0.2],
            [1684965593, 'BTC_EOS', 'upbit', 'binance', 0.007, 0.3]]


def profit_setting_mock():
    return {
        'min_profit_percent': 0.03,
        'min_profit_btc': 0.0075,
        'is_withdraw': False
    }


def transaction_mock():
    return {
        'BTC_ETH': 0.002,
        'BTC_XRP': 1,
        'BTC_QTUM': 0.5,
        'BTC_ADA': 1,
    }


def trading_fee_mock():
    return {
        'BTC_ETH': 0.0001,
        'BTC_XRP': 0.002,
        'BTC_QTUM': 0.003,
        'BTC_ADA': 0.001,
    }


def primary_balance_mock():
    return {
        'BTC': 0.5,
        'ETH': 2,
        'XRP': 1000,
        'EOS': 100,
        'ADA': 500
    }


def secondary_balance_mock():
    return {
        'BTC': 0.3,
        'ETH': 1,
        'XRP': 100,
        'ADA': 500,
        'QTUM': 100,
    }


def currencies_mock():
    return [
        'BTC', 'ETH', 'XRP', 'ADA', 'QTUM'
    ]
