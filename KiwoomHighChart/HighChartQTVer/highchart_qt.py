import queue
import enum
import copy

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import pyqtSignal
from PyQt5 import QtCore
from PyQt5 import (
     QtWidgets, uic, QtGui
)


from pyqtgraph import PlotWidget, AxisItem

from Util.pyinstaller_patch import *
from Util.pyinstaller_patch_gui import LoginWidget
from StockApis.kiwoom import KiwoomAPIModule, TRADE_AMOUNT

main_ui = uic.loadUiType(os.path.join(sys._MEIPASS, 'gui/main.ui'))[0]


"""

"""

if 'pydevd' in sys.modules:
    DEBUG = True
else:
    DEBUG = False

DEBUG = False
debugger.debug("DEBUG: {}".format(DEBUG))

MARKET_START = 9 * 60
MARKET_END = 15 * 60 + 20


class Commands(enum.Enum):
    REGISTER_GET_FILLED_DATA = 'register_get_filled_data'
    GET_FILLED_DATA = 'get_filled_data'
    REMOVE_GET_FILLED_DATA = 'remove_get_filled_data'
    GET_BULK_FILLED_DATA = 'get_bulk_filled_data'


def is_trading_time():
    if DEBUG:
        return False
    
    now_date = datetime.datetime.now()
    current_time = now_date.hour * 60 + now_date.minute
    return True if MARKET_START <= current_time <= MARKET_END else False


def get_number_from_label_text(label):
    return int(label.text().replace(',', '').replace('백만원', '') or 0)


class KiwoomGraphCalculator(QtCore.QThread):
    refresh_graph = QtCore.pyqtSignal(list)

    def __init__(self, q):
        super(KiwoomGraphCalculator, self).__init__()

        self.data_q = q
        
        self.stop_flag = False
        self.pause_flag = False
        
        self.default_set()
        
    def default_set(self):
        self._c4f4_time_list = list()
        self._c4f4_time_str_list = list()
        self._c4f4_acc_list = list()
        self._c4f4_last_emit_time = time.time()

        self._i4l4_time_list = list()
        self._i4l4_time_str_list = list()
        self._i4l4_acc_list = list()
        self._i4l4_last_emit_time = time.time()

        self._o4r4_time_list = list()
        self._o4r4_time_str_list = list()
        self._o4r4_acc_list = list()
        self._o4r4_last_emit_time = time.time()

    def run(self):
        while not self.stop_flag:
            try:
                flag_list, data_set = self.data_q.get(timeout=30)
                for flag in flag_list:
                    self.data_setter(flag, data_set)
            except:
                for flag in ['c4f4', 'i4l4', 'o4r4']:
                    if 'c4f4' in flag:
                        time_str_list, acc_list = self._c4f4_time_str_list, self._c4f4_acc_list
                    elif 'i4l4' in flag:
                        time_str_list, acc_list = self._i4l4_time_str_list, self._i4l4_acc_list
                    else:  # 'o4r4' in flag:
                        time_str_list, acc_list = self._o4r4_time_str_list, self._o4r4_acc_list
                    reversed_time_str = reversed(time_str_list)
                    reversed_acc_list = reversed(acc_list)
                    self.refresh_graph.emit([flag, reversed_time_str, reversed_acc_list])

    def stop(self):
        self.stop_flag = True
        
    def pause(self):
        self.pause_flag = True

    def resume(self):
        self.pause_flag = False
        self.default_set()

    def data_setter(self, flag, data_set):
        # 매수체결량합 + 매도체결량 합 ( o4r4_sum_label )
        # 양수합 + 음수합 ( c4f4_sum_label )
        # 양수합 + 음수합 ( i4l4_sum_label )

        if 'c4f4' in flag:
            time_list, time_str_list, acc_list = self._c4f4_time_list, self._c4f4_time_str_list, self._c4f4_acc_list
            last_emit_time = self._c4f4_last_emit_time
            filled_data = int(data_set['filled_amount'])

        elif 'i4l4' in flag:
            time_list, time_str_list, acc_list = self._i4l4_time_list, self._i4l4_time_str_list, self._i4l4_acc_list
            last_emit_time = self._i4l4_last_emit_time
            filled_data = int(data_set['filled_amount'])

        else:  # 'o4r4' in flag:
            time_list, time_str_list, acc_list = self._o4r4_time_list, self._o4r4_time_str_list, self._o4r4_acc_list
            last_emit_time = self._o4r4_last_emit_time
            filled_data = int(data_set['duplicate_filled_amount'])
        now_time = datetime.datetime.strptime(data_set['filled_time'][:4], '%H%M')
        now_time_str = now_time.strftime('%H:%M')
        if not time_list:
            time_list.append(now_time)
            time_str_list.append(now_time_str)
            acc_list.append(0)
        else:
            if now_time not in time_list:
                for n, time_ in enumerate(time_list):
                    if now_time > time_:
                        time_list.insert(n, now_time)
                        time_str_list.insert(n, now_time_str)
                        acc_list.insert(n, acc_list[n])
                        break
                else:
                    time_list.append(now_time)
                    time_str_list.append(now_time_str)
                    acc_list.append(0)

            for n, time_ in enumerate(time_list):
                if time_ >= now_time:
                    acc_list[n] += filled_data
                else:
                    # time_list is sorted that once time_ is smaller than now_time, rest of the list will be as well
                    break
        if time.time() > last_emit_time + 10:
            if 'c4f4' in flag:
                self._c4f4_last_emit_time = time.time()
            elif 'i4l4' in flag:
                self._i4l4_last_emit_time = time.time()
            else:
                self._o4r4_last_emit_time = time.time()
            reversed_time_str = reversed(time_str_list)
            reversed_acc_list = reversed(acc_list)
            self.refresh_graph.emit([flag, reversed_time_str, reversed_acc_list])
        
        
class KiwoomOrderSuccessMonitor(QtWidgets.QWidget, main_ui):
    closed = QtCore.pyqtSignal()
    graph_setter_signal = QtCore.pyqtSignal(dict)

    def __init__(self, _id=None, *args, **kwargs):
        super(KiwoomOrderSuccessMonitor, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self._id = _id
        
        self.command_q = queue.Queue()
        self.data_q = queue.Queue()
        self.calculate_queue = queue.Queue()
        self.stock_code_dict = dict()
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.stock_code_combo_box.setEnabled(False)
        
        self.buttons_connection()
        
        self._buy_table_set = dict()
        self._sell_table_set = dict()
        
        self._buy_order_filtered_table_set = dict()
        self._sell_order_filtered_table_set = dict()
        
        self.thread_setter()

        self._duplicate_intersections = set()
        self._flag_set = list()
        
        self.buy_order_success_table.setColumnWidth(1, 60)
        self.buy_order_filtered_table.setColumnWidth(1, 60)
        self.sell_order_success_table.setColumnWidth(1, 60)
        self.sell_order_filtered_table.setColumnWidth(1, 60)
        self.duplicate_filtered_table.setColumnWidth(1, 60)
        self.duplicate_filtered_table.setColumnWidth(4, 60)

        self.debug_total = 0

    # SAI Login
    def closeEvent(self, QCloseEvent):
        close_program(self._id)
        self.closed.emit()
    
    def graph_setter(self, data_list):
        flag, time_str_list, acc_list = data_list
        
        if 'c4f4' in flag:
            graph = self.c4f4_graph
        elif 'i4l4' in flag:
            graph = self.i4l4_graph
        else:  # o4r4
            graph = self.o4r4_graph
        
        left = [item // 1000 for item in acc_list]
        bottom = [[(n, str(item) if '00' in item else str()) for n, item in enumerate(time_str_list)]]
        graph.getAxis('bottom').setTicks(bottom)
        graph.plotItem.clear()
        graph.plotItem.plot(left)
        
    def communicates(self, callback_queue, data):
        if data == Commands.REGISTER_GET_FILLED_DATA:
            data = dict(
                command=Commands.REGISTER_GET_FILLED_DATA,
                stock_code_list=self.stock_code_list,
                fid_list=[TRADE_AMOUNT],
            )
        elif data == Commands.REMOVE_GET_FILLED_DATA:
            data = dict(
                command=Commands.REMOVE_GET_FILLED_DATA,
                stock_code_list=self.stock_code_list,
            )
        elif data == Commands.GET_BULK_FILLED_DATA:
            data = dict(
                command=Commands.GET_BULK_FILLED_DATA,
                stock_code_list=self.stock_code_list,
            )
        self.command_q.put((callback_queue, data))
    
    def thread_setter(self):
        self._kiwoom_thread = KiwoomCommunicateThread(self.command_q)
        self._calcuator = CalculateThread(self.calculate_queue)
        self._graph_calculator = KiwoomGraphCalculator(self.data_q)
        self._thread_list = [self._kiwoom_thread, self._calcuator, self._graph_calculator]

        self._kiwoom_thread.kiwoom_all_stock_name_receiver.connect(self.stock_code_combo_setter)
        self._calcuator.line_data_signal.connect(self.line_setter)
        self._calcuator.table_signal.connect(self.table_handler)
        self._calcuator.queue_signal.connect(self.communicates)
        
        self._graph_calculator.refresh_graph.connect(self.graph_setter)

        for thread in self._thread_list:
            debugger.debug('start thread, [{}]'.format(thread.__str__()))
            thread.start()

    def buttons_connection(self):
        self.start_btn.clicked.connect(self.start_btn_setter)
        self.stop_btn.clicked.connect(self.stop_btn_setter)
    
    def stock_code_combo_setter(self, stock_code_dict):
        self.start_btn.setEnabled(True)
        self.stock_code_combo_box.setEnabled(True)
        self.stock_code_dict = stock_code_dict
        
    def start_btn_setter(self):
        debugger.debug('start btn clicked.')
        filter_ = self.filter_combobox.currentText()
        
        if '없음' in filter_:
            self.digits_filter = None
        elif '10' in filter_:
            self.digits_filter = 100
        else:
            self.digits_filter = 10
        
        self.minimum_amount = self.min_success_amount_spinbox.text()
        self.mininum_count = int(self.min_success_count_spinbox.text())
        self.stock_code_list = [self.stock_code_combo_box.currentText().split(' ')[0]] if not DEBUG else ['005930']
        
        debugger.debug('digits_fillter=[{}], minimum_amount=[{}], self.mininum_count=[{}], stock_code_list={}'.format(
            self.digits_filter, self.minimum_amount, self.mininum_count, self.stock_code_list
        ))
        
        if not self.stock_code_list:
            debugger.debug('stock_code_list must be not null.')
            return

        stock_code = self.stock_code_list[0]
        idx = self.stock_code_combo_box.findData(stock_code)
        if idx > 0:
            self.stock_code_combo_box.removeItem(idx)
            self.stock_code_combo_box.insertItem(
                0, '{} {}'.format(stock_code, self.stock_code_dict.get(stock_code, '')),
                userData=self.stock_code_list[0]
            )
        elif idx < 0:
            # never existed
            self.stock_code_combo_box.insertItem(
                0, '{} {}'.format(stock_code, self.stock_code_dict.get(stock_code, '')),
                userData=self.stock_code_list[0]
            )

        self.stock_code_combo_box.setCurrentIndex(0)
        
        self.stop_btn.setEnabled(True)
        self.start_btn.setEnabled(False)
        self.filter_combobox.setEnabled(False)
        self.min_success_amount_spinbox.setEnabled(False)
        self.min_success_count_spinbox.setEnabled(False)
        self.stock_code_combo_box.setEnabled(False)

        self._buy_table_set = dict()
        self._sell_table_set = dict()

        self._buy_order_filtered_table_set = dict()
        self._sell_order_filtered_table_set = dict()
        
        self._duplicate_intersections = set()
        self._flag_set = list()

        self.all_table_clear()
        self._calcuator.resume()
        self._graph_calculator.resume()
        self.communicates(self.calculate_queue, Commands.REGISTER_GET_FILLED_DATA)
        
        now_date = datetime.datetime.now()
        current_time = now_date.hour * 60 + now_date.minute
        if DEBUG or (MARKET_START > current_time or current_time > MARKET_END):
            self.communicates(self.calculate_queue, Commands.GET_BULK_FILLED_DATA)

        if DEBUG:
            self._thread_list[0].debug_running_stop = False

    def stop_btn_setter(self):
        debugger.debug('stop btn clicked.')

        self.stop_btn.setEnabled(False)
        self.start_btn.setEnabled(True)
        self.filter_combobox.setEnabled(True)
        self.min_success_amount_spinbox.setEnabled(True)
        self.min_success_count_spinbox.setEnabled(True)
        self.stock_code_combo_box.setEnabled(True)

        # for thread in self._thread_list:
        #     debugger.debug('stop thread, [{}]'.format(thread.__str__()))
        #     thread.stop()
        if DEBUG:
            self._thread_list[0].debug_running_stop = True

        self.communicates(self.calculate_queue, Commands.REMOVE_GET_FILLED_DATA)
        self._calcuator.pause()
        self._graph_calculator.pause()
        
    def line_setter(self, data_set):
        # debugger.debug('get line_setter event..')
        self.last_updated_time_label.setText(
            ':'.join([data_set['filled_time'][:2], data_set['filled_time'][2:4], data_set['filled_time'][4:6]])
        )
        self.current_price_label.setText('{:,}원'.format(int(data_set['price'])))
        self.diff_percent_label.setText('{:.2f}%'.format(float(data_set['fluctuation_per'] or 0)))
    
    def all_table_clear(self):
        table_list = [
            self.buy_order_success_table,
            self.buy_order_filtered_table,
            self.sell_order_success_table,
            self.sell_order_filtered_table,
            self.duplicate_filtered_table
        ]
        
        for table in table_list:
            table.setRowCount(0)
    
    def _base_item_setter(self, row, table, data_set, color=None):
        for num, each in enumerate(data_set):
            item = table.item(row, num)
            if not item:
                item = QtWidgets.QTableWidgetItem(str(each))
                if color:
                    item.setBackground(color)
                table.setItem(row, num, item)
            else:
                item.setText(str(each))

    def minimum_count_table_setter(self, filled, price, table, table_set, sum_label, avg_label, mul_label, is_reverse,
                                   should_filter, table_type):
        price = abs(price)
        default = [1, filled, filled * price]
    
        if filled in table_set:
            table_set[filled][0] += 1
            table_set[filled][1] += filled
            table_set[filled][2] += filled * price

            if table_set[filled][0] > int(self.mininum_count):
                index = table.findItems(str(filled), QtCore.Qt.MatchExactly)
                if index and index[0].column() == 0:
                    row = index[0].row()
                    count, total, _ = table_set[filled]
                    data = [filled, count, total]
                    color = None
                    if should_filter and total % self.digits_filter == 0:
                        color = QtGui.QColor(135, 206, 250)
                    self._base_item_setter(row, table, data, color)
        else:
            table_set.setdefault(filled, default)

        if table_set[filled][0] == int(self.mininum_count):
            total = self.mininum_count * filled
            default_items = [filled, self.mininum_count, total]
            row_count = table.rowCount()

            if row_count == 0:
                table.insertRow(0)
                color = None
                if should_filter and total % self.digits_filter == 0:
                    color = QtGui.QColor(135, 206, 250)
                self._base_item_setter(0, table, default_items, color)
            else:
                for row in range(row_count):
                    now_ = abs(int(table.item(row, 0).text()))
                    if abs(filled) > now_:
                        table.insertRow(row)
                        color = None
                        if should_filter and total % self.digits_filter == 0:
                            color = QtGui.QColor(135, 206, 250)
                        self._base_item_setter(row, table, default_items, color)
                        break
                else:
                    row = row + 1
                    table.insertRow(row)
                    color = None
                    if should_filter and total % self.digits_filter == 0:
                        color = QtGui.QColor(135, 206, 250)
                    self._base_item_setter(row, table, default_items, color)
                    
        if table_set[filled][0] >= int(self.mininum_count):
            if table_set[filled][0] == int(self.mininum_count):
                self._graph_data_set['filled_amount'] = self.mininum_count * filled
            if table_type == 'success':
                self._flag_set.append('c4f4')
            elif table_type == 'filtered':
                self._flag_set.append('i4l4')

        total_sum = sum([total for count, total, price in table_set.values() if count >= int(self.mininum_count)])
        total_price = sum([price for count, total, price in table_set.values() if count >= int(self.mininum_count)])

        if not total_price or not total_sum:
            return

        avg = abs(total_price // total_sum)

        total_sum_item = QtWidgets.QTableWidgetItem(str(total_sum))
        table.setHorizontalHeaderItem(2, total_sum_item)

        sum_label.setText(str(total_sum))
        avg_label.setText(str(avg))
        mul_label.setText('{:,}백만원'.format(total_sum * avg // 1000000))

        self.c4f4_sum_label.setText(
            str(int(self.c_sum_label.text() or 0) + int(self.f_sum_label.text() or 0))
        )
        self.c2f2_sum_label.setText(
            '{:,}백만원'.format(get_number_from_label_text(self.a2b2_mul_label) + get_number_from_label_text(self.d2e2_mul_label))
        )
        self.i4l4_sum_label.setText(
            str(int(self.i_sum_label.text() or 0) + int(self.l_sum_label.text() or 0))
        )
        self.i2l2_sum_label.setText(
            '{:,}백만원'.format(get_number_from_label_text(self.g2h2_mul_label) + get_number_from_label_text(self.j2k2_mul_label))
        )

    def table_setter(self, filled, price, table, table_set, sum_label, avg_label, mul_label, is_reverse, should_filter,
                     table_type):
        price = abs(price)
        if filled in table_set:
            table_set[filled][0] += 1
            table_set[filled][1] += filled
            table_set[filled][2] += filled * price

            index = table.findItems(str(filled), QtCore.Qt.MatchExactly)
            if index and index[0].column() == 0:
                row = index[0].row()
                count, total, _ = table_set[filled]
                data = [filled, count, total]
                color = None
                if should_filter and total % self.digits_filter == 0:
                    color = QtGui.QColor(135, 206, 250)
                self._base_item_setter(row, table, data, color)
        else:
            default = [1, filled, filled * price]
            table_set.setdefault(filled, default)
            default_items = [filled, 1, filled]
            row_count = table.rowCount()

            if row_count == 0:
                table.insertRow(0)
                self._base_item_setter(0, table, default_items)
            else:
                for row in range(row_count):
                    now_ = abs(int(table.item(row, 0).text()))
                    if abs(filled) > now_:
                        table.insertRow(row)
                        color = None
                        if should_filter and filled % self.digits_filter == 0:
                            color = QtGui.QColor(135, 206, 250)
                        self._base_item_setter(row, table, default_items, color)
                        break
                else:
                    row = row + 1
                    table.insertRow(row)
                    color = None
                    if should_filter and filled % self.digits_filter == 0:
                        color = QtGui.QColor(135, 206, 250)
                    self._base_item_setter(row, table, default_items, color)

        if table_type == 'success':
            self._flag_set.append('c4f4')

        elif table_type == 'filtered':
            self._flag_set.append('i4l4')

        total_sum = sum([total for count, total, price in table_set.values()])
        total_price = sum([price for count, total, price in table_set.values()])
        avg = abs(total_price // total_sum)

        total_sum_item = QtWidgets.QTableWidgetItem(str(total_sum))
        table.setHorizontalHeaderItem(2, total_sum_item)

        sum_label.setText(str(total_sum))
        avg_label.setText(str(avg))
        mul_label.setText('{:,}백만원'.format(total_sum * avg // 1000000))

        self.c4f4_sum_label.setText(
            str(int(self.c_sum_label.text() or 0) + int(self.f_sum_label.text() or 0))
        )
        self.c2f2_sum_label.setText(
            '{:,}백만원'.format(get_number_from_label_text(self.a2b2_mul_label) + get_number_from_label_text(self.d2e2_mul_label))
        )
        self.i4l4_sum_label.setText(
            str(int(self.i_sum_label.text() or 0) + int(self.l_sum_label.text() or 0))
        )
        self.i2l2_sum_label.setText(
            '{:,}백만원'.format(get_number_from_label_text(self.g2h2_mul_label) + get_number_from_label_text(self.j2k2_mul_label))
        )

    def duplicate_table_setter(self, filled, intersections):
        intersections = set(intersections)
        table = self.duplicate_filtered_table
        filled = abs(filled)
        if filled in self._duplicate_intersections:
            index = table.findItems(str(filled), QtCore.Qt.MatchExactly)
            if index and index[0].column() == 0:
                row = index[0].row()
                buy_count, buy_total, _ = self._buy_order_filtered_table_set[filled]
                sell_count, sell_total, _ = self._sell_order_filtered_table_set[-filled]
                # if not buy_count >= int(self.mininum_count) or not sell_count >= int(self.mininum_count):
                #     return
                data = [filled, buy_count, buy_total, -filled, sell_count, sell_total]
                self._base_item_setter(row, table, data)

        else:
            difference = sorted(intersections.difference(self._duplicate_intersections), reverse=True)
            for key in difference:
                key = abs(key)
                row_count = table.rowCount()

                buy_count, buy_total, buy_price = self._buy_order_filtered_table_set[key]
                sell_count, sell_total, sell_price = self._sell_order_filtered_table_set[-key]
                data = [key, buy_count, buy_total, -key, sell_count, sell_total]

                if not buy_count >= int(self.mininum_count) or not sell_count >= int(self.mininum_count):
                    continue

                self._duplicate_intersections.add(key)
                if row_count == 0:
                    table.insertRow(0)
                    self._base_item_setter(0, table, data)
                else:
                    for row in range(row_count):
                        now_ = abs(int(table.item(row, 0).text()))
                        if key > now_:
                            table.insertRow(row)
                            self._base_item_setter(row, table, data)
                            break
                    else:
                        row = row + 1
                        table.insertRow(row)
                        self._base_item_setter(row, table, data)

        # if not self._duplicate_intersections:
        #     self._duplicate_intersections = intersections

        buy_total_price = int()
        buy_total_filled = int()

        sell_total_price = int()
        sell_total_filled = int()
        for key in self._duplicate_intersections:
            buy_count, buy_total, buy_price = self._buy_order_filtered_table_set[key]
            sell_count, sell_total, sell_price = self._sell_order_filtered_table_set[-key]
        
            buy_total_price += buy_price
            buy_total_filled += buy_total

            sell_total_price += sell_price
            sell_total_filled += sell_total

        avg = (buy_total_price // buy_total_filled) if buy_total_filled else 0
        total_sum_item = QtWidgets.QTableWidgetItem(str(buy_total_filled))
        minus_avg = abs(sell_total_price // sell_total_filled) if sell_total_filled else 0

        minus_total_sum_item = QtWidgets.QTableWidgetItem(str(sell_total_filled))

        self.duplicate_filtered_table.setHorizontalHeaderItem(2, total_sum_item)
        self.duplicate_filtered_table.setHorizontalHeaderItem(5, minus_total_sum_item)

        self.o_sum_label.setText(str(buy_total_filled))
        self.o_avg_label.setText(str(avg))
        self.m2n2_mul_label.setText('{:,}백만원'.format(buy_total_filled * avg // 1000000))

        self.r_sum_label.setText(str(sell_total_filled))
        self.r_avg_label.setText(str(minus_avg))
        self.p2q2_mul_label.setText('{:,}백만원'.format(sell_total_filled * minus_avg // 1000000))

        self.o4r4_sum_label.setText(str(buy_total_filled + sell_total_filled))
        self.o2r2_sum_label.setText('{:,}백만원'.format((buy_total_filled * avg + sell_total_filled * minus_avg) // 1000000))

    def table_handler(self, list_):
        # debugger.debug('get table_handler event.. {}'.format(list_))
        data_set, table_type = list_

        if table_type == 'buy':
            order_success_table = self.buy_order_success_table
            order_success_table_set = self._buy_table_set
            order_success_sum_label = self.c_sum_label
            order_success_avg_label = self.c_avg_label
            order_success_mul_label = self.a2b2_mul_label

            order_filtered_table = self.buy_order_filtered_table
            order_filtered_table_set = self._buy_order_filtered_table_set
            order_filtered_sum_label = self.i_sum_label
            order_filtered_avg_label = self.i_avg_label
            order_filtered_mul_label = self.g2h2_mul_label
            is_reverse = True
            
        else:
            order_success_table = self.sell_order_success_table
            order_success_table_set = self._sell_table_set
            order_success_sum_label = self.f_sum_label
            order_success_avg_label = self.f_avg_label
            order_success_mul_label = self.d2e2_mul_label
            
            order_filtered_table = self.sell_order_filtered_table
            order_filtered_table_set = self._sell_order_filtered_table_set
            order_filtered_sum_label = self.l_sum_label
            order_filtered_avg_label = self.l_avg_label
            order_filtered_mul_label = self.j2k2_mul_label
            is_reverse = False

        filled = int(data_set['filled_amount'])
        price = int(data_set['price'])
        self._flag_set = list()
        self._graph_data_set = copy.deepcopy(data_set)
        if int(self.minimum_amount) > abs(filled):
            return

        should_filter = self.digits_filter is not None and filled % self.digits_filter == 0

        # 체결량 테이블
        if int(self.mininum_count) == 0:
            self.table_setter(
                filled, price, order_success_table, order_success_table_set,
                order_success_sum_label, order_success_avg_label, order_success_mul_label,
                is_reverse, should_filter, 'success'
            )
        else:
            self.minimum_count_table_setter(
                filled, price, order_success_table, order_success_table_set,
                order_success_sum_label, order_success_avg_label, order_success_mul_label,
                is_reverse, should_filter, 'success'
            )
        # 첫번째 필터링 테이블
        if should_filter:
            self.data_q.put((self._flag_set, self._graph_data_set))
            return

        if int(self.mininum_count) == 0:
            self.table_setter(
                filled, price, order_filtered_table, order_filtered_table_set,
                order_filtered_sum_label, order_filtered_avg_label, order_filtered_mul_label,
                is_reverse, should_filter, 'filtered'
            )
        else:
            self.minimum_count_table_setter(
                filled, price, order_filtered_table, order_filtered_table_set,
                order_filtered_sum_label, order_filtered_avg_label, order_filtered_mul_label,
                is_reverse, should_filter, 'filtered'
            )
        # 두번째 필터링 테이블
        abs_sell_order_filtered = set(map(abs, self._sell_order_filtered_table_set.keys()))
        intersections = abs_sell_order_filtered.intersection(set(self._buy_order_filtered_table_set))
        should_color = abs(filled) not in self._duplicate_intersections and abs(filled) in intersections
        
        if intersections and abs(filled) in intersections:
            self.duplicate_table_setter(filled, intersections)
            buy_count, buy_total, buy_price = self._buy_order_filtered_table_set[abs(filled)]
            sell_count, sell_total, sell_price = self._sell_order_filtered_table_set[-abs(filled)]

            if (filled > 0 and buy_count == self.mininum_count and sell_count >= self.mininum_count) or (filled < 0 and buy_count >= self.mininum_count and sell_count == self.mininum_count):
                duplicate_total_filled = buy_total + sell_total
                self._graph_data_set['duplicate_filled_amount'] = duplicate_total_filled
                self._flag_set.append('o4r4')
                # print("first: {} - {}".format(filled, duplicate_total_filled))
                # self.debug_total += duplicate_total_filled
            elif buy_count >= int(self.mininum_count) and sell_count >= int(self.mininum_count):
                # 문제, dup table은 양수음수 카운트 값이 minimum보다 클때 둘다 한꺼번에 노출되어 계산됨
                # 이 부분에서 뭔가 꼬인거같음;;
                # 과거로 갈수록 값이 점점 커지는게 문제같음(?)
                self._graph_data_set['duplicate_filled_amount'] = filled
                self._flag_set.append('o4r4')
                # print("again: {}".format(filled))
                # self.debug_total += filled

        # 필터링 테이블 컬러링
        if should_color:
            try:
                buy_item = [
                    item for item in self.buy_order_filtered_table.findItems(str(abs(filled)), QtCore.Qt.MatchExactly)
                    if item.column() == 0
                ][0]
                sell_item = [
                    item for item in self.sell_order_filtered_table.findItems(str(-abs(filled)), QtCore.Qt.MatchExactly)
                    if item.column() == 0
                ][0]
                buy_item.setBackground(QtGui.QColor(255, 255, 0))
                sell_item.setBackground(QtGui.QColor(154, 205, 50))
            except Exception:
                pass

        self.data_q.put((self._flag_set, self._graph_data_set))
        # print("debug_total: {}".format(self.debug_total))
        # print("o4r4_sum: {}".format(self.o4r4_sum_label.text()))


class KiwoomCommunicateThread(QtCore.QThread):
    kiwoom_all_stock_name_receiver = pyqtSignal(dict)
    
    def __init__(self, command_q):
        super(KiwoomCommunicateThread, self).__init__()
        self._command_q = command_q
        self._stock_code_set_q = queue.Queue()
        
        self.kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.kiwoom.dynamicCall("CommConnect()")
        self.kiwoom_api = KiwoomAPIModule(self.kiwoom)
        self.stop_flag = False
        if DEBUG:
            self.debug_running_stop = False
        self.connections()
    
    def __str__(self):
        return 'KiwoomCommunicateThread'
    
    def connections(self):
        self.kiwoom.OnReceiveTrData.connect(self.kiwoom_api.receive_tx_data)
        self.kiwoom.OnReceiveChejanData.connect(self.kiwoom_api.receive_chejan_data)
        self.kiwoom.OnEventConnect.connect(self.kiwoom_api.connect_status_receiver)

        self.event_thread = QtCore.QThread()
        self.event_thread.start()
        
        self.kiwoom_api.moveToThread(self.event_thread)

        self.kiwoom.OnReceiveRealData.connect(self.kiwoom_api.receive_real_tx_data)
        self.kiwoom.OnReceiveMsg.connect(self.kiwoom_api.receive_msg)
        self.kiwoom.OnReceiveConditionVer.connect(self.kiwoom_api.receive_condition_ver)
        self.kiwoom.OnReceiveRealCondition.connect(self.kiwoom_api.receive_real_condition)

    def run(self):
        for _ in range(30):
            if not self.kiwoom_api.is_connected:
                time.sleep(1)
                continue
            res = self.get_all_stock_korean_name()
            self.kiwoom_all_stock_name_receiver.emit(res)
            break
        
        while not self.stop_flag:
            try:
                callback_queue, data = self._command_q.get(timeout=2)
                # debugger.debug('get command, [{}]'.format(data))
                if data['command'] == Commands.REGISTER_GET_FILLED_DATA:
                    self.kiwoom_api.registry_real_stock_filled_data(
                        data['stock_code_list'],
                        callback_queue,
                        data['fid_list'],
                    )
                elif data['command'] == Commands.REMOVE_GET_FILLED_DATA:
                    self.kiwoom_api._first_bulk_filled_flag = True
                    self.kiwoom_api.remove_real_stock_filled_data(
                        data['stock_code_list']
                    )

                elif data['command'] == Commands.GET_BULK_FILLED_DATA:
                    self.kiwoom_api.get_bulk_filled_data(
                        data['stock_code_list'],
                        callback_queue,
                    )

            except:
                continue
        
        debugger.debug('{}, Closed.'.format(self.__str__()))

    def get_all_stock_korean_name(self):
        code_dic = self.kiwoom_api.get_stock_codes()
        kospi_codes = self.get_index_stock_names('9542', code_dic['kospi'])
        kosdaq_codes = self.get_index_stock_names('9543', code_dic['kosdaq'])
        
        return {**kosdaq_codes, **kospi_codes}
        
    def get_index_stock_names(self, scn, codes):
        indexes = list()
        for i in range(0, len(codes), 100):
            indexes.append(i)
        end = len(codes)
        indexes.reverse()
        totals = dict()
        for index in indexes:
            code_list = codes[index:end]
            self.kiwoom_api.get_all_stock_korean_name(scn, code_list, self._stock_code_set_q)
            res = self._stock_code_set_q.get(timeout=20)
            totals.update(res)
            end = index
            time.sleep(0.2)
        return totals

    def stop(self):
        self.stop_flag = True


class CalculateThread(QtCore.QThread):
    line_data_signal = pyqtSignal(dict)
    table_signal = pyqtSignal(list)
    queue_signal = pyqtSignal(queue.Queue, enum.Enum)
    
    def __init__(self, trade_queue):
        super(CalculateThread, self).__init__()

        self.stop_flag = False
        self.pause_flag = False
        self._trade_queue = trade_queue
        self._first_real_data_set = None
        
        if DEBUG:
            self._first_real_data_set = {
                'filled_time': '153030',
                'price': '53500',
                'fluctuation_per': '3.5',
                'filled_amount': '300',
                'acu_amount': '1842502'
            }
        
    def __str__(self):
        return 'CalculateThread'

    def run(self):
        while not self.stop_flag:
            try:
                receive_type, data = self._trade_queue.get(timeout=2)
                if receive_type == 'real_tx_data':
                    data = data.split('\t')
                    data_set = {
                        'filled_time': data[0],
                        'price': data[1],
                        'fluctuation_per': data[3],
                        'filled_amount': data[6],
                        'acu_amount': data[7]
                    }
                    if self._first_real_data_set is None:
                        if is_trading_time():
                            self._first_real_data_set = data_set
                            self.queue_signal.emit(self._trade_queue, Commands.GET_BULK_FILLED_DATA)
                else:
                    if data == dict() and not self.pause_flag:
                        self.queue_signal.emit(self._trade_queue, Commands.GET_BULK_FILLED_DATA)
                    else:
                        if is_trading_time():
                            if int(data['acu_amount']) > int(self._first_real_data_set['acu_amount']):
                                continue
                    data_set = data
                if data_set:
                    if int(data_set['filled_amount']) > 0:
                        self.table_signal.emit([data_set, 'buy'])
                    else:
                        self.table_signal.emit([data_set, 'sell'])
                    self.line_data_signal.emit(data_set)
            except:
                continue

        debugger.debug('{}, Closed.'.format(self.__str__()))

    def stop(self):
        self.stop_flag = True
        self._first_real_data_set = None

    def pause(self):
        self.pause_flag = True

    def resume(self):
        self._first_real_data_set = None
        self.pause_flag = False

