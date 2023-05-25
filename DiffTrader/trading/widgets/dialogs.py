import os

from PyQt5 import (QtWidgets)

from DiffTrader.trading.widgets.utils import save, load
from DiffTrader.paths import DialogWidgets as widgets
from DiffTrader.messages import (QMessageBoxMessage as Msg)

from Util.pyinstaller_patch import debugger


class SettingEncryptKeyDialog(QtWidgets.QDialog, widgets.KEY_DIALOG_WIDGET):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        
        self.save_exchange = None
        self.kwargs = dict()

        self.btn.rejected.connect(self.close)
        self.btn.accepted.connect(self.save)
    
    def show_encrypt(self, exchange, **kwargs):
        self.save_exchange = exchange
        self.kwargs = kwargs
        
        self.show()
    
    def save(self):
        key = self.enc_key.text()
        success = save(self.save_exchange, key, **self.kwargs)
        if not success:
            self.gui = DifferentKeyInputDialog(self.save_exchange, key, **self.kwargs)
            self.gui.show()
        else:
            log = (Msg.Title.SAVE_RESULT, Msg.Content.SAVE_SUCCESS)
            debugger.debug(log)
            QtWidgets.QMessageBox.about(self, *log)
            self.close()


class DifferentKeyInputDialog(QtWidgets.QDialog, widgets.CONFIRM_DIALOG_WIDGET):
    def __init__(self, exchange, password, **kwargs):
        super().__init__()
        self.setupUi(self)

        self.btn.rejected.connect(self.close)
        self.btn.accepted.connect(lambda: self.save(exchange, password, **kwargs))

    def save(self, exchange, password, **kwargs):
        os.remove('Settings')
        while os.path.exists('Settings'):
            pass
        success = save(exchange, password, **kwargs)
        log = (Msg.Title.SAVE_RESULT, Msg.Content.SAVE_SUCCESS if success else Msg.Content.SAVE_FAIL)
        debugger.debug(log)
        QtWidgets.QMessageBox.about(self, *log)
        self.close()


class LoadSettingsDialog(QtWidgets.QDialog, widgets.KEY_DIALOG_WIDGET):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.data = None
        self.btn.rejected.connect(self.close)
        self.btn.accepted.connect(self.set_data)

    def set_data(self):
        success = load(self.enc_key.text())
        if not success:
            box = QtWidgets.QMessageBox()
            box.setIcon(QtWidgets.QMessageBox.Question)
            box.setWindowTitle(Msg.Title.FAIL_LOAD)
            box.setText(Msg.Content.WRONG_SECRET_KEY)

            debugger.debug(Msg.Title.FAIL_LOAD, Msg.Content.WRONG_SECRET_KEY)

            box.setStandardButtons(QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
            buttonY = box.button(QtWidgets.QMessageBox.Yes)
            buttonY.setText('확인')
            buttonN = box.button(QtWidgets.QMessageBox.No)
            buttonN.setText('취소')
            res = box.exec_()
            if res == 16384:
                os.remove('Settings')
                self.close()
                self.data = False
        else:
            self.data = success

    def exec(self):
        if os.path.exists('Settings'):
            while self.data is None:
                super().exec()
            return self.data
        else:
            return False
