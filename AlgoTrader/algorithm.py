from datetime import datetime,date
import numpy as np
from datetime import datetime
import time
import requests
from Huobi.huobi_modi import Huobi
from Binance.binance import Binance
from pythonds.basic.stack import Stack
from Upbit import upbit
import re


class BaseAlgorithm:
    def __init__(self):
        super().__init__()
        self.data = {}
        self.settings = {}
        self.old_data = {}
        self.exchange = None

    def buy(self, values):
        # return {"coinA": True, "coinB": False .....}
        pass

    def sell(self, values):
        # return {"coinA": True, "coinB": False .....}
        pass

    def calculate(self, candles):
        # return {"coinA": value, "coinB": value .....}
        pass

    def get_sma(self, value):
        sma = np.sum(value) / len(value)

        return sma

    def node_check(self, contents):
        p_cnt = 0

        oper_dict = {}
        for c, i in enumerate(contents):
            if i == '(':
                p_cnt += 1
                continue

            elif i == ')':
                p_cnt -= 1
                continue

            if p_cnt not in oper_dict:
                oper_dict[p_cnt] = ''

            oper_dict[p_cnt] = '{}{}'.format(oper_dict[p_cnt], i)

        return sorted(oper_dict.items())

    def build_node(self, contents):
        for k, con in contents[::-1]:
            if 'AND' in con:
                nodes = con.split('AND')
                val, left, right = 'AND', nodes[0], nodes[1]

            else:
                nodes = con.split('OR')
                val, left, right = 'OR', nodes[0], nodes[1]

            print(val, left, right)


class SMAAlgorithm(BaseAlgorithm):
    def __init__(self):
        super().__init__()

    def get_sma(self, value):
        sma = np.sum(value) / len(value)

        return sma


class GoldenCross(SMAAlgorithm):
    def __init__(self, settings, exchange):
        super().__init__()
        self.settings = settings
        self.exchange = exchange

    def check_values(self, values):
        if values['yet_short_sma'] >= values['yet_long_sma']:

            if values['short_sma'] >= values['long_sma']:
                return True

            else:
                return False
        else:
            return False

    # def buy(self, values):
    #
    #     if values['yet_short_sma'] >= values['yet_long_sma']:
    #
    #         if values['short_sma'] >= values['long_sma']:
    #             return True
    #
    #         else:
    #             return False
    #     else:
    #         return False
    #
    # def sell(self, values):
    #     # return {"coinA": True, "coinB": False .....}
    #     pass

    def calculate(self, symbol):
        suc, candles, msg = self.exchange.get_candle(symbol, self.settings['candle_size'],
                                                     self.settings['long_period'] + 1)

        if not suc:
            return suc, candles, msg

        short_close = candles['close'][:self.settings['short_period']]
        long_close = candles['close'][:self.settings['long_period']]

        prev_short_close = candles['close'][1:self.settings['short_period']+1]
        prev_long_close = candles['close'][:self.settings['long_period']+1]

        prev_short_sma = self.get_sma(prev_short_close)
        prev_long_sma = self.get_sma(prev_long_close)

        short_sma = self.get_sma(short_close)
        long_sma = self.get_sma(long_close)

        res = {
            'short_sma': short_sma,
            'long_sma': long_sma,
            'prev_short_sma': prev_short_sma,
            'prev_long_sma': prev_long_sma
        }

        return True, res, ''


class BollingerBand(SMAAlgorithm):
    def __init__(self, settings, exchange):
        super().__init__()

        self.settings = settings
        self.exchange = exchange

    def check_values(self, values):

        previous_index = 'prev_{}_band'.format(self.settings['reference']).lower()
        index = 'last_{}_band'.format(self.settings['reference']).lower()

        if self.settings['trade_method'] == 'cross_bound_upper':
            # 'cross_band_upper' lower -> upper 로 band를 cross할 때

            if values['prev_candle'] < values[previous_index] and values['last_candle'] > values[index]:
                return True

            else:
                return False

        elif self.settings['trade_method'] == 'cross_bound_lower':
            # 'cross_band_lower' upper -> lower 로 band를 cross 할 때
            if values['prev_candle'] > values[previous_index] and values['last_candle'] < values[index]:
                return True

            else:
                return False

    # def buy(self, values):
    #     return self.check_values(values)
    #
    # def sell(self, values):
    #     return self.check_values(values)

    def calculate(self, symbol):
        suc, candles, msg = self.exchange.get_candle(symbol, self.settings['candle_size'], self.settings['period'] + 1)

        if not suc:
            return suc, candles, msg

        candles = candles['close']

        self.old_data = self.get_band_index(candles[:-1])
        latest_bands = self.get_band_index(candles[1:])

        res = {
            'last_middle_band': latest_bands['middle'],
            'last_upper_band': latest_bands['upper'],
            'last_lower_band': latest_bands['lower'],
            'last_candle': candles[0],
            'prev_middle_band': self.old_data['middle'],
            'prev_upper_band': self.old_data['upper'],
            'prev_lower_band': self.old_data['lower'],
            'prev_candle': candles[1]
            }

        return True, res, ''

    def get_band_index(self, candles):

        sma = self.get_sma(candles)
        deviation = self.get_deviation(candles)

        middle_band = sma
        upper_band = sma + (deviation * self.settings['deviation'])
        lower_band = sma - (deviation * self.settings['deviation'])

        return {'middle': middle_band, 'upper': upper_band, 'lower': lower_band}

    def get_deviation(self, value):
        average = np.sum(value) / len(value)

        sub_average = np.subtract(value, average)
        exponent = np.power(sub_average, 2)

        result = np.sum(exponent) / (len(value)-1)

        deviation = np.sqrt(abs(result))

        return deviation


class RelativeStrengthIndex(BaseAlgorithm):
    def __init__(self, settings, exchange):
        super().__init__()
        self.settings = settings
        self.exchange = exchange

    def check_values(self, values):
        if self.settings['trade_method'] == 'bound_upper':
            # 현재 bound가 rsi보다 upper 상황에 구매인 경우.

            if self.settings['bound'] > values['rsi']:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'bound_lower':
            # 현재 bound가 rsi보다 lower 상황에 구매인 경우.

            if self.settings['bound'] < values['rsi']:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'cross_bound_upper':
            # 현재 bound가 rsi보다 lower->upper 상황에 구매인 경우.

            #  if self.settings['bound'] < values['prev_rsi'] and self.settings['bound'] > values['rsi']:

            if values['prev_rsi'] < self.settings['bound'] < values['rsi']:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'cross_bound_lower':
            # 현재 bound가 rsi보다 upper->lower 상황에  구매인 경우.

            #  if self.settings['bound'] > values['prev_rsi'] and self.settings['bound'] < values['rsi']:

            if values['rsi'] < self.settings['bound'] < values['prev_rsi']:
                return True
            else:
                return False

    # def buy(self, values):  # rsi가 high->low인경우, low->high인경우, 단지 high인경우, low인경우
    #     return self.check_values(values)
    # def sell(self, values):
    #     return self.check_values(values)

    def calculate(self, symbol):
        success, candles, msg = self.exchange.get_candle(symbol, self.settings['candle_size'], 200)
        if not success:
            return False, '', msg

        '''
        First Average Gain = Sum of Gains over the past 14 periods / 14.
        First Average Loss = Sum of Losses over the past 14 periods / 14
        Average Gain = [(previous Average Gain) x 13 + current Gain] / 14.
        Average Loss = [(previous Average Loss) x 13 + current Loss] / 14.
        '''

        period = self.settings['period']

        differences = np.array(candles['close'][1:]) - np.array(candles['close'][:-1])
        prev_average_gain = np.sum(differences[:period][differences[:period] > 0]) / period
        prev_average_loss = np.sum(differences[:period][differences[:period] < 0]) / period

        for diff in differences[period:-1]:
            prev_average_gain = ((prev_average_gain * (period - 1)) + (0 if diff < 0 else diff)) / period
            prev_average_loss = ((prev_average_loss * (period - 1)) - (0 if diff > 0 else diff)) / period

        prev_rsi = self.get_rsi(prev_average_gain, prev_average_loss)

        average_gain = ((prev_average_gain * (period - 1)) + (0 if differences[-1] < 0 else differences[-1])) / period
        average_loss = ((prev_average_loss * (period - 1)) - (0 if differences[-1] > 0 else differences[-1])) / period
        rsi = self.get_rsi(average_gain, average_loss)

        res = {'rsi': rsi, 'prev_rsi': prev_rsi}

        return True, res, ''

    def get_rsi(self, gain, loss):
        rs = abs(gain / loss)

        rsi = 100 - (100 / (1 + rs))

        return rsi


class MACD(BaseAlgorithm):
    def __init__(self, settings, exchange):
        super().__init__()
        self.settings = settings
        self.exchange = exchange

    def check_values(self, values):
        if self.settings['reference'] == 'line':
            # 참조 형태가 macdLine인 경우
            index, pervious_index = values['macd_line'], values['previous_macd_line']
        else:
            # 참조 형태가 Histogram인 경우
            index, pervious_index = values['macd_histogram'], values['previous_macd_histogram']

        if self.settings['trade_method'] == 'bound_upper':
            # 설정한 경계선보다 높아지면 매매
            if pervious_index < self.settings['bound'] < index:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'bound_lower':
            # 설정한 경계선보다 낮아지면 매매
            if index < self.settings['bound'] < pervious_index:
                return True
            else:
                return False

    # def buy(self, values):
    #     return self.check_values(values)
    #
    # def sell(self, values):
    #     return self.check_values(values)

    def calculate(self, symbol):
        suc, candles, msg = self.exchange.get_candle(symbol, self.settings['candle_size'], 100)

        if not suc:
            return suc, candles, msg

        candles = candles['close']

        sync = self.settings['long_period'] - self.settings['short_period']
        short_ema_list = self.get_ema(candles, self.settings['short_period'])[sync:]
        long_ema_list = self.get_ema(candles, self.settings['long_period'])

        differ = np.array(short_ema_list) - np.array(long_ema_list)

        previous_macd_line = differ[:-1]
        macd_line = differ

        signal_differ = self.get_ema(macd_line, self.settings['signal_period'])

        previous_signal_line = signal_differ[:-1]
        signal_line = signal_differ

        previous_macd_histogram = previous_macd_line[-1] - previous_signal_line[-1]
        macd_histogram = macd_line[-1] - signal_line[-1]

        if self.settings['reference'] == 'line':
            res = {
                'previous_macd_line': previous_macd_line[-1],
                'macd_line': macd_line[-1],
            }

        elif self.settings['reference'] == 'histogram':
            res = {
                'previous_macd_histogram': previous_macd_histogram,
                'macd_histogram': macd_histogram
            }

        return True, res, ''

    def get_ema(self, candles, period):
        previous_ema = (np.sum(candles[:period]) / period)
        multipliers = (2 / (period + 1))
        ema_list = []

        for candle in candles[period+1:]:
            previous_ema = (candle - previous_ema) * multipliers + previous_ema

            ema_list.append(previous_ema)

        return ema_list


class Stochastic(BaseAlgorithm):

    def __init__(self, settings, exchange):
        super().__init__()

        self.settings = settings
        self.exchange = exchange

    def check_values(self, values):
        if self.settings['reference'] == 'fastk':
            index, previous_index = values['fast_k'], values['previous_fask_k']
        else:
            #  settings가 slowD인 경우.
            index, previous_index = values['slow_d'], values['previous_slow_d']

        if self.settings['trade_method'] == 'bound_upper':
            # 현재 bound가 value보다 upper 상황에 구매인 경우.
            if self.settings['bound'] > index:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'bound_lower':
            # 현재 bound가 value보다 lower 상황에 구매인 경우.
            if self.settings['bound'] < index:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'cross_bound_upper':
            # 현재 bound가 value보다 lower->upper 상황에 구매인 경우.
            if previous_index < self.settings['bound'] < index:

                return True
            else:
                return False

        elif self.settings['trade_method'] == 'cross_bound_lower':
            # 현재 bound가 value보다 upper->lower 상황에  구매인 경우.
            if index < self.settings['bound'] < previous_index:
                return True
            else:
                return False

    # def buy(self, values):
    #     return self.check_values(values)
    #
    # def sell(self, values):
    #     return self.check_values(values)

    def calculate(self, symbol):
        suc, candles, msg = self.exchange.get_candle(symbol, self.settings['candle_size'], self.settings['period']+5)

        if not suc:
            return suc, candles, msg

        period = self.settings['period']

        high, low, close = candles['high'], candles['low'], candles['close']

        highist_high = np.max(high[:period-1])
        lowest_low = np.min(low[:period-1])

        k_list = []
        for n in range(6):
            if highist_high < high[period - 1 + n]:
                highist_high = high[period - 1 + n]

            elif lowest_low > low[period - 1 + n]:
                lowest_low = low[period - 1 + n]

            k = ((close[period - 1 + n] - lowest_low) / (highist_high - lowest_low)) * 100

            k_list.append(k)

        stoc_list = []
        for m in range(4):
            # stoc_list 순서 최신->구
            stochastic = self.get_sma(value=k_list[m:m+3])

            stoc_list.append(stochastic)

        slow_d = self.get_sma(value=stoc_list[:2])
        previous_slow_d = self.get_sma(value=stoc_list[:-1])
        res = {
            'fast_k': k_list[-1],
            'previous_fask_k': k_list[-2],
            'slow_d': slow_d,
            'previous_slow_d': previous_slow_d
        }

        return True, res, ''


class CCI(BaseAlgorithm):

    def __init__(self, settings, exchange):
        super().__init__()

        self.settings = settings
        self.exchange = exchange

    def check_values(self, values):
        if self.settings['trade_method'] == 'bound_upper':
            # 현재 bound가 cci보다 upper 상황에 구매인 경우.

            if self.settings['bound'] > values['cci']:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'bound_lower':
            # 현재 bound가 cci보다 lower 상황에 구매인 경우.

            if self.settings['bound'] < values['cci']:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'cross_bound_upper':
            # 현재 bound가 cci보다 lower->upper 상황에 구매인 경우.

            if values['prev_cci'] < self.settings['bound'] < values['cci']:
                return True
            else:
                return False

        elif self.settings['trade_method'] == 'cross_bound_lower':
            # 현재 bound가 cci보다 upper->lower 상황에  구매인 경우.

            if values['cci'] < self.settings['bound'] < values['prev_cci']:
                return True
            else:
                return False

    # def buy(self, values):
    #     return self.check_values(values)
    #
    # def sell(self, values):
    #     return self.check_values(values)

    def calculate(self, symbol):

        suc, candles, msg = self.exchange.get_candle(symbol, self.settings['candle_size'], self.settings['period']+1)

        if not suc:
            return suc, candles, msg

        period = self.settings['period']
        all_sum = np.array(candles['high']) + np.array(candles['low']) + np.array(candles['close'])
        typical_price = np.divide(all_sum, 3)

        cci_list = []
        for i in range(2):
            period_of_sma = self.get_sma(typical_price[i:period + i])

            distance = np.absolute(np.subtract(period_of_sma, typical_price[i:period + i]))

            mean_deviation = np.sum(distance) / period
            subtract_typical = typical_price[-2 + i] - period_of_sma
            cci = np.divide(subtract_typical, 0.015 * mean_deviation)

            cci_list.append(cci)
        # 첫 20일의 Typical Price를 구한다.

        res = {
            'previous_cci': cci_list[0],
            'cci': cci_list[1]
        }

        return True, res, ''


class Node:
    def __init__(self, val, left, right):
        self.l = self.convert_string(left)
        self.r = self.convert_string(right)
        self.v = val

    def convert_string(self, data):
        # 분봉 / 기간 / 표준편차 / 참조 형태/ 매매방법
        # 분봉 = candle_size, 기간 = period
        # 매매방법 = trade_method, 참조 선 = bound
        # 참조형태 = reference
        upbit_key = ''
        upbit_secret = ''

        exchange = upbit.BaseUpbit(upbit_key, upbit_secret)

        settings = {}

        if not data:
            return None
        elif data.v == 'AND' or data.v == 'OR':
            # AND, OR가 부모노드인 경우는 노드가 완성된 경우밖에 없다.
            return data

        data = re.findall('[\d\w]+', data.v.lower())

        algo_name = data[0]
        setting_words = data[1:]

        # 볼린저, rsi, macd, stoc, cci
        # 분봉은 무조건 첫번째, 매매방법은 항상 마지막.
        settings['candle_size'] = setting_words[0]
        settings['trade_method'] = setting_words[-1]

        if 'GoldenCross' in data:
            GoldenCross(settings, exchange)

        elif 'band' in algo_name:
            # 볼린저 밴드
            settings['period'] = int(setting_words[1])
            settings['deviation'] = int(setting_words[2])
            settings['reference'] = setting_words[3]

            return BollingerBand(settings, exchange)

        elif 'rsi' in algo_name:
            # RSI
            settings['period'] = int(setting_words[1])
            settings['bound'] = int(setting_words[2])

            return RelativeStrengthIndex(settings, exchange)

        elif 'macd' in algo_name:
            # MACD
            settings['short_period'] = int(setting_words[1])
            settings['long_period'] = int(setting_words[2])
            settings['signal_period'] = int(setting_words[3])
            settings['bound'] = int(setting_words[4])
            settings['reference'] = setting_words[5]

            return MACD(settings, exchange)

        elif 'stoch' in algo_name:
            # Stochastic
            settings['period'] = int(setting_words[1])
            settings['bound'] = int(setting_words[2])
            settings['reference'] = setting_words[3]

            return Stochastic(settings, exchange)

        elif 'cci' in algo_name:
            # CCI
            settings['period'] = int(setting_words[1])
            settings['bound'] = int(setting_words[2])

            return CCI(settings, exchange)


class Tree:
    def __init__(self):
        self.root = None

    def unpack(self, content):
        if content.startswith("(") and content.endswith(")"):
            content = content[1:-1]

        return content

    def build_node(self, contents):
        if contents is None:
            return None

        val, left, right = self.node_text(contents)
        n = Node(val, self.build_node(left), self.build_node(right))

        return n

    def node_text(self, contents):
        p_cnt = 0
        for num, c in enumerate(contents):
            if c == 'A' and contents[num:num + 3] == 'AND' and p_cnt == 0:
                return 'AND', self.unpack(contents[:num]), self.unpack(contents[num + 3:])

            elif c == 'O' and contents[num:num + 2] == 'OR' and p_cnt == 0:
                return 'OR', self.unpack(contents[:num]), self.unpack(contents[num + 2:])

            elif c == '(':
                p_cnt += 1

            elif c == ')':
                p_cnt -= 1

        return contents, None, None

    def node_calculate(self, node, coin):

        n_suc, n_data, n_msg = node.calculate(coin)

        if not n_suc:
            return False

        res = node.check_values(n_data)

        return res

    def node_inspection(self, node, coin):
        # 객체가 존재하지 않는경우
        for cal in [node.l, node.r]:
            if type(cal) == Node:
                # 노드 값인경우 계산이 안되므로 continue처리
                continue

            cal_res = self.node_calculate(cal, coin)

            if node.v == 'AND':

                if cal_res:
                    continue

                else:
                    return False

            else:  # node.v == 'OR':

                if cal_res:
                    return True

                else:
                    continue

        else:
            if node.v == 'AND':
                return True

            else:
                return False

    def node_search(self, node, coin):
        if type(node.l) == Node:
            res = self.node_search(node.l, coin)

            if node.v == 'OR' and res:
                return True

            elif node.v == 'AND' and not res:
                return False

        if type(node.r) == Node:
            res = self.node_search(node.r, coin)

            return res

        return self.node_inspection(node, coin)

        # res = self.node_inspection(node)
        # list_.append(res)
        #
        # if node.v == 'AND':
        #     if not res:
        #         return False
        #
        # else:
        #     if res:
        #         return True


if __name__ == '__main__':
    t_case1 = '(Stochastic[1, 14, 50, fastK, bound_lower] AND CCI[1, 14, 50, bound_upper]) OR ((BollingerBand[1, 20, 2, upper, cross_bound_upper] AND MACD[1, 12, 26, 9, 50, Histogram, bound_upper] AND MACD[1, 13, 26, 9, 100, Histogram, bound_upper]) OR (BollingerBand[1, 30, 2, upper, cross_bound_upper]))'.replace(' ', '')
    t_case2 = '(Stochastic[1, 14, 50, fastK, bound_lower] AND CCI[1, 14, 50, bound_upper]) OR ((BollingerBand[1, 20, 2, upper, cross_bound_upper] AND MACD[1, 12, 26, 9, 50, Histogram, bound_upper] AND MACD[1, 13, 26, 9, 100, Histogram, bound_upper]) OR (BollingerBand[1, 30, 2, upper, cross_bound_upper]))'.replace(' ', '')
    # t_case = '(Stochastic(1, 14, 50, fastK, bound_lower) AND CCI(1, 14, 50, bound_upper)) OR (((BollingerBand(1, 20, 2, upper, cross_bound_upper) AND MACD(1, 12, 26, 9, 50, Histogram, bound_upper) AND MACD(1, 13, 26, 9, 100, Histogram, bound_upper)) OR (BollingerBand(1, 30, 2, upper, cross_bound_upper))))'
    # 봉 = 1, 기간 = 14, 매매라인 = 50, 어떨때 판매? = bound_lower..
    # TestCase
    # (BollingerBand(1, 20, 2, upper, cross_bound_upper) AND MACD(1, 12, 26, 9, 50, Histogram, bound_upper) AND MACD(1, 13, 26, 9, 100, Histogram, bound_upper))
    # OR (BollingerBand(1, 30, 2, upper, cross_bound_upper))
    # Stochastic(1, 14, 50, fastK, bound_lower) OR CCI(1, 14, 50, bound_upper)
    # BollingerBand(1, 20, 2, upper, cross_bound_upper) OR MACD(1, 12, 26, 9, 50, Histogram, bound_upper)

    tree = Tree()
    a = tree.build_node(t_case)
    b = tree.node_search(a, coin)
