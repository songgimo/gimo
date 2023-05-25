from PyQt5 import (QtWidgets, QtCore)

from DiffTrader.paths import ProgramSettingWidgets
from DiffTrader.trading.apis import (save_total_data_to_database, load_total_data_to_database,
                                     get_expected_profit)
from DiffTrader.trading.settings import AVAILABLE_EXCHANGES, ENABLE_SETTING, UNABLE_SETTING
from DiffTrader.trading.widgets.dialogs import SettingEncryptKeyDialog, LoadSettingsDialog
from DiffTrader.trading.widgets.utils import base_item_setter, number_type_converter
from DiffTrader.trading.threads.trade_thread import TradeThread
from DiffTrader.trading.threads.sender import SenderThread
from DiffTrader.messages import QMessageBoxMessage as Msg
from DiffTrader.settings import DEBUG

from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QApplication

from Util.pyinstaller_patch import debugger, close_program


import logging
import queue

"""
    controller로 보내야 하는 기준 명확하게 정의해야함.
    dynamic하게 변경 필요함.
    For the next clean up
        sending expected profit should be done in a different thread? sending a POST may cost a lot
        remove one way exchanges and related logics
"""


class TradeObject(object):
    def __init__(self, trade_date, symbol, primary_exchange, secondary_exchange, profit_btc, profit_percent):
        self.trade_date = trade_date
        self.symbol = symbol
        self.primary_exchange = primary_exchange
        self.secondary_exchange = secondary_exchange
        self.profit_btc = profit_btc
        self.profit_percent = profit_percent


class DiffTraderGUI(QtWidgets.QMainWindow, ProgramSettingWidgets.DIFF_TRADER_WIDGET):
    closed = QtCore.pyqtSignal()
    message_signal = pyqtSignal(tuple)

    def __init__(self, _id, email, parent=None):
        super().__init__()

        self.user_id = _id
        self.email = email
        self.parent = parent

        self.data_receive_queue = queue.Queue()
        self.sender_thread = SenderThread(self.data_receive_queue)
        self.sender_thread.start()

        self.setupUi(self)
        
        # connect buttons
        self.startTradeBtn.clicked.connect(self.start_trade)
        self.stopTradeBtn.clicked.connect(self.stop_trade)

        # define tab widgets
        self._main_tab = self.MainTab(self)
        self._exchange_setting_tab = self.ExchangeSettingTab(self)
        self._program_setting_tab = self.ProgramSettingTab(self)

        self.stopTradeBtn.setEnabled(False)

    def closeEvent(self, *args, **kwargs):
        close_program(self.user_id)
        self.top_profit_thread.exit()
        self.closed.emit()
    
    def _set_to_ready_trading(self):
        """
            enable stop btn for stopping trade_thread,
            unable start btn for preventing doubleclick and etc.
        """
        self.startTradeBtn.setEnabled(False)
        self.stopTradeBtn.setEnabled(True)

    def _trade_validation_checker(self, primary_settings, secondary_settings, profit_settings):
        """
            It is validator settings are validated or not.

            Args:
                primary_settings: It is primary exchange setting that includes exchange's key and secret
                secondary_settings: It is secondary exchange setting that includes exchange's key and secret
                profit_settings: It is profit setting that includes profit percent, BTC and auto withdrawal.
            Return:
                True if all settings are validated else False and emitting message to user.
        """
        if primary_settings is None or secondary_settings is None:
            QtWidgets.QMessageBox.warning(self,
                                          Msg.Title.EXCHANGE_SETTING_ERROR,
                                          Msg.Content.REQUIRE_EXCHANGE_SETTING)
            return False

        elif self.primaryExchange.currentText() == self.secondaryExchange.currentText():
            QtWidgets.QMessageBox.warning(self,
                                          Msg.Title.EXCHANGE_SETTING_ERROR,
                                          Msg.Content.CANNOT_BE_SAME_EXCHANGE)
            return False

        elif not profit_settings:
            QtWidgets.QMessageBox.warning(self,
                                          Msg.Title.EXCHANGE_SETTING_ERROR,
                                          Msg.Content.WRONG_PROFIT_SETTING)
            return False

        return True

    def start_trade(self):
        """
            It is TradingThread starter.
        """
        profit_settings = self._program_setting_tab.profit_settings

        primary_settings = self._exchange_setting_tab.config_dict.get(self.primaryExchange.currentText(), None)
        secondary_settings = self._exchange_setting_tab.config_dict.get(self.secondaryExchange.currentText(), None)

        if not self._trade_validation_checker(primary_settings, secondary_settings, profit_settings):
            return

        min_profit_percent = profit_settings['min_profit_percent']
        min_profit_btc = profit_settings['min_profit_btc']
        auto_withdrawal = profit_settings['auto_withdrawal']

        self._set_to_ready_trading()
        self.trade_thread = TradeThread(
            email=self.email,
            primary_info=primary_settings,
            secondary_info=secondary_settings,
            min_profit_per=min_profit_percent,
            min_profit_btc=min_profit_btc,
            auto_withdrawal=auto_withdrawal,
            primary_name=self.primaryExchange.currentText(),
            secondary_name=self.secondaryExchange.currentText(),
            data_receive_queue=self.data_receive_queue
        )

        self.trade_thread.log_signal.connect(self._main_tab.write_logs)
        self.trade_thread.stopped.connect(self.trade_thread_is_stopped)

        self.trade_thread.start()

    def stop_trade(self):
        """
            Stop and close TradingThread when signal is received by stop_btn
            stop btn -> trying to stop from TradingThread
            -> go to trade_thread_is_stopped when TradingThread is stopped.
        """
        if self.trade_thread and self.trade_thread.isAlive():
            self.trade_thread.stop()
            self._main_tab.write_logs('거래 중지를 시도합니다.')

    def trade_thread_is_stopped(self):
        self.startTradeBtn.setEnabled(True)
        self.stopTradeBtn.setEnabled(False)
        self._main_tab.write_logs('거래가 중지되었습니다.')

    class MainTab(object):
        """
            It is a tab to start trading after selecting two widgets.
            Also it is located with profit table, profit top 10 table, log widget.
        """
        def __init__(self, diff_gui):
            """
                Args:
                    diff_gui: diffTraderGUI(object)
            """
            self._diff_gui = diff_gui
            self._user_id = diff_gui.user_id
            self._email = diff_gui.email
            self._parent = diff_gui.parent

            # define table variables
            self.trade_object_set = set()

            # exchange select bar settings
            self._diff_gui.primaryExchange.addItems(AVAILABLE_EXCHANGES)
            self._diff_gui.secondaryExchange.addItems(AVAILABLE_EXCHANGES[::-1])

            self._diff_gui.primaryExchange.currentIndexChanged.connect(lambda: self.same_exchange_checker(
                self._diff_gui.secondaryExchange
            ))
            self._diff_gui.secondaryExchange.currentIndexChanged.connect(lambda: self.same_exchange_checker(
                self._diff_gui.primaryExchange
            ))
            
            self.set_trade_object_set_from_server()

        def initiation_for_set_table(self):
            self.set_all_trade_history()
            self.top_ten_by_profits()
        
        def same_exchange_checker(self, exchange_combobox):
            """
                Check the exchange is selected twice from primary and secondary.
                And move to other exchange for preventing set the same exchange.

                Args:
                    exchange_combobox: selected comboBox
            """
            if self._diff_gui.primaryExchange.currentText() == self._diff_gui.secondaryExchange.currentText():
                box_item_length = exchange_combobox.count()
                selected_index = exchange_combobox.currentIndex()

                move_to_index = selected_index + 1
                if move_to_index >= box_item_length:
                    move_to_index = selected_index - 1

                exchange_combobox.setCurrentIndex(move_to_index)

        def set_trade_history(self, trade_object):
            """
                It is historyView setter when trading is done and received its data.

                Args:
                    trade_object: TradeObject, data object.

            """
            self._diff_gui.profitPercent.text()

            item_list = [
                trade_object.trade_date,
                trade_object.symbol,
                trade_object.primary_exchange,
                trade_object.secondary_exchange,
                trade_object.profit_btc,
                trade_object.profit_percent,
            ]
            row_count = self._diff_gui.tradeHistoryView.rowCount()
            base_item_setter(row_count, self._diff_gui.tradeHistoryView, item_list)

            btc_total = [each.profit_btc for each in self.trade_object_set]
            percent_total = [each.profit_percent for each in self.trade_object_set]
            
            total_profit_btc = sum(btc_total)
            total_profit_percent = sum(percent_total) / len(percent_total)
            self._diff_gui.profitBTC.setText(total_profit_btc)
            self._diff_gui.profitPercent.setText(total_profit_percent)

        def set_all_trade_history(self):
            """
                It is setting all historyView setter when program is starting and receiving data from profit server.
            """
            row_count = self._diff_gui.tradeHistoryView.rowCount()
            for trade_object in self.trade_object_set:
                data_list = [
                    trade_object.trade_date,
                    trade_object.symbol,
                    trade_object.primary_exchange,
                    trade_object.secondary_exchange,
                    trade_object.profit_btc,
                    trade_object.profit_percent,
                ]
                self._diff_gui.tradeHistoryView.insertRow(row_count)
                base_item_setter(row_count, self._diff_gui.tradeHistoryView, data_list)
                row_count += 1

        def update_tables(self, trade_object):
            """
                Update trade_history table, top 10 by profit table
                after trading and getting history object from trade_thread.

                Args:
                    trade_object: TradeObject, data object
            """
            self.trade_object_set.add(trade_object)

            self.set_trade_history(trade_object)
            self.top_ten_by_profits()

        def set_trade_object_set_from_server(self):
            """
                It is self.trade_object_set setting function.
            """
            def after_process(result_data_list):
                for data_list in result_data_list:
                    trade_date, symbol, primary_exchange, secondary_exchange, profit_btc, profit_percent = data_list
        
                    trade_object = TradeObject(
                        trade_date,
                        symbol,
                        primary_exchange,
                        secondary_exchange,
                        profit_btc,
                        profit_percent
                    )
                    self.trade_object_set.add(trade_object)
                self.initiation_for_set_table()
                
            get_expected_profit(self._user_id, self._diff_gui.data_receive_queue, after_process)

        def top_ten_by_profits(self):
            """
                It is profitRankView setter when trading is done and received its data.
            """
            sorted_objects = sorted(self.trade_object_set, key=lambda x: x.profit_btc, reverse=True)
            
            row_count = self._diff_gui.profitRankView.rowCount()

            for trade_object in sorted_objects:
                item_list = [
                    trade_object.trade_date,
                    trade_object.symbol,
                    trade_object.primary_exchange,
                    trade_object.secondary_exchange,
                    trade_object.profit_btc,
                    trade_object.profit_percent,
                ]
                self._diff_gui.profitRankView.insertRow(row_count)
                base_item_setter(row_count, self._diff_gui.profitRankView, item_list)
                row_count += 1

        def write_logs(self, msg, level=logging.INFO):
            """
                It is display TradingThread logs.
            """
            debugger.log(level, msg)
            self._diff_gui.logBox.setText(
                '\n'.join(self._diff_gui.logBox.toPlainText().split('\n')[-500:]) + '\n' + str(msg)
            )
            # self._diff_gui.logBox.verticalScrollBar().setValue(
            #     self._diff_gui.logBox.verticalScrollBar().maximum())

    class ExchangeSettingTab(object):
        def __init__(self, diff_gui):
            """
                It is a tab for setting key, secret
                Args:
                    diff_gui: diffTraderGUI(object)
            """
            self._diff_gui = diff_gui
            self._user_id = diff_gui.user_id
            self._email = diff_gui.email
            self._parent = diff_gui.parent

            self.config_dict = dict()

            self._diff_gui.bithumbLocalSaveBtn.clicked.connect(self.local_save)
            self._diff_gui.upbitLocalSaveBtn.clicked.connect(self.local_save)
            self._diff_gui.binanceLocalSaveBtn.clicked.connect(self.local_save)

            self._diff_gui.bithumbShowSecretCheckbox.clicked.connect(lambda: self.show_secret(
                self._diff_gui.bithumbShowSecretCheckbox, self._diff_gui.bithumbSecret
            ))
            self._diff_gui.upbitShowSecretCheckbox.clicked.connect(lambda: self.show_secret(
                self._diff_gui.upbitShowSecretCheckbox, self._diff_gui.upbitSecret
            ))
            self._diff_gui.binanceShowSecretCheckbox.clicked.connect(lambda: self.show_secret(
                self._diff_gui.binanceShowSecretCheckbox, self._diff_gui.binanceSecret
            ))

            self.dialog = SettingEncryptKeyDialog()
            self.load_dialog = LoadSettingsDialog()

            self.load_key_secret()

        def load_key_secret(self):
            setting_data = self.load_dialog.exec()
            for exchange_name in AVAILABLE_EXCHANGES:
                # setting_data 값을 가져올 때 exchange들은 lower 값
                lower_exchange = exchange_name.lower()
                if setting_data and lower_exchange in setting_data.keys():
                    if exchange_name == 'Bithumb':
                        key_box, secret_box = self._diff_gui.bithumbKey, self._diff_gui.bithumbSecret
                    elif exchange_name == 'Upbit':
                        key_box, secret_box = self._diff_gui.upbitKey, self._diff_gui.upbitSecret
                    elif exchange_name == 'Binance':
                        key_box, secret_box = self._diff_gui.binanceKey, self._diff_gui.binanceSecret

                    key, secret = setting_data[lower_exchange]['key'], setting_data[lower_exchange]['secret']

                    key_box.setText(key)
                    secret_box.setText(secret)

                    exchange_config = {exchange_name: {
                        'key': key,
                        'secret': secret
                    }}

                    self.config_dict.update(exchange_config)

        def show_secret(self, show_secret_box, secret_box):
            index = 0 if show_secret_box.isChecked() else 2
            secret_box.setEchoMode(index)

        def local_save(self):
            """
                button 클릭시 상위 group box 가져옴 -> groupBox의 name, 하위의 line edits(key, secret) 값 추출
            """
            parent_widget = self._diff_gui.sender().parent()
            exchange_name = parent_widget.objectName()

            key, secret = [each.text() for each in parent_widget.findChildren(QtWidgets.QLineEdit)]

            if not key or not secret:
                QtWidgets.QMessageBox.warning(self._diff_gui,
                                              Msg.Title.EXCHANGE_SETTING_ERROR,
                                              Msg.Content.WRONG_KEY_SECRET)

                return

            else:
                self.dialog.show_encrypt(exchange_name, key=key, secret=secret)

            exchange_config = {exchange_name: {
                'key': key,
                'secret': secret
            }}

            self.config_dict.update(exchange_config)

            # QtWidgets.QMessageBox.warning(self._diff_gui,
            #                               Msg.Title.SAVE_RESULT,
            #                               Msg.Content.SAVE_SUCCESS)

    class ProgramSettingTab(object):
        def __init__(self, diff_gui):
            """
                Args:
                    diff_gui: diffTraderGUI(object)
            """
            self._diff_gui = diff_gui
            self._user_id = diff_gui.user_id
            self._email = diff_gui.email
            self._parent = diff_gui.parent

            self._diff_gui.saveProgramSettingBtn.clicked.connect(self.save_profit_settings)
            self._diff_gui.message_signal.connect(self.test_emit)

            self.profit_settings = self.load_and_set_profit_settings()

        def load_and_set_profit_settings(self):
            def after_process(result_dict):
                if not result_dict:
                    return dict()
                else:
                    self._diff_gui.minProfitPercent.setValue(result_dict['min_profit_percent'] * 100)
                    self._diff_gui.minProfitBTC.setValue(result_dict['min_profit_btc'])

                    if result_dict['auto_withdrawal'] is True:
                        self._diff_gui.autoWithdrawal.setCurrentText(ENABLE_SETTING)
                    else:
                        self._diff_gui.autoWithdrawal.setCurrentText(UNABLE_SETTING)

                    self.profit_settings = result_dict
            load_total_data_to_database(self._user_id, self._diff_gui.data_receive_queue, after_process)

        def test_emit(self, tuple_):
            title, contents = tuple_
            QtWidgets.QMessageBox.about(self._diff_gui, title, contents)

        def save_profit_settings(self):
            def after_process(result):
                if result:
                    self._diff_gui.message_signal.emit((Msg.Title.SAVE_RESULT, Msg.Content.SAVE_SUCCESS_TO_SERVER))
                    return
                else:
                    self._diff_gui.message_signal.emit((Msg.Title.SAVE_RESULT, Msg.Content.SAVE_FAIL_TO_SERVER))
                    return

            min_profit_percent_str = self._diff_gui.minProfitPercent.text()
            min_profit_btc_str = self._diff_gui.minProfitBTC.text()
            auto_withdrawal = True if self._diff_gui.autoWithdrawal.currentText() == ENABLE_SETTING else False
            
            min_profit_percent = number_type_converter(int, min_profit_percent_str)
            min_profit_btc = number_type_converter(float, min_profit_btc_str)
            
            if min_profit_percent <= 0:
                QtWidgets.QMessageBox.warning(self._diff_gui,
                                              Msg.Title.EXCHANGE_SETTING_ERROR,
                                              Msg.Content.WRONG_PROFIT_PERCENT)
                return dict()
            elif min_profit_btc <= 0:
                QtWidgets.QMessageBox.warning(self._diff_gui,
                                              Msg.Title.EXCHANGE_SETTING_ERROR,
                                              Msg.Content.WRONG_PROFIT_BTC)
                return dict()

            min_profit_percent_to_float = min_profit_percent / 100
            self.profit_settings = dict(
                min_profit_percent=min_profit_percent_to_float,
                min_profit_btc=min_profit_btc,
                auto_withdrawal=auto_withdrawal
            )
            save_total_data_to_database(self._user_id, min_profit_percent_to_float, min_profit_btc, auto_withdrawal,
                                        self._diff_gui.data_receive_queue, after_process)


