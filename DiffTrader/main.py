import os
import sys
import requests
import hashlib

from PyQt5 import uic
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSignal, QThread
from DiffTrader.settings import LOGIN_URL, UPDATE_IP_URL, VERSION, DEBUG
from DiffTrader.trading.widgets.main_diff_trader import DiffTraderGUI
from DiffTrader.paths import LoginWidgets
from DiffTrader.messages import (QMessageBoxMessage as Msg)

from Util.pyinstaller_patch import debugger, check_status, evt


class LoginWidget(QWidget, LoginWidgets.LOGIN_WIDGET):
    def __init__(self, pid, widget_after_login):
        super().__init__()
        if DEBUG:
            pid = 'SAIDiffTrader'
        self.setupUi(self)
        self.pid = hashlib.sha256(pid.encode()).hexdigest()
        self.widget_after_login = widget_after_login

        self.passwordEdit.returnPressed.connect(self.sign_in)
        self.loginBtn.clicked.connect(self.sign_in)

    def sign_in(self):
        if DEBUG:
            id_ = '533'
            email = ''
            data = dict(id=id_)
            self.after_login(data, email)
        else:
            if self.is_valid_form():
                username = self.idEdit.text()
                password = self.passwordEdit.text()

                self.submit(username, password)

    def is_valid_form(self):
        if self.idEdit.text() == '':
            QMessageBox.about(self, Msg.Title.LOGIN_FAILED, Msg.Content.EMPTY_ID)
            return False

        if self.passwordEdit.text() == '':
            QMessageBox.about(self, Msg.Title.LOGIN_FAILED, Msg.Content.EMPTY_PASSWORD)
            return False

        return True

    def after_login(self, data, email):
        t = StatusCheck(data['id']) #threading.Thread(target=check_status, args=(data['id'], evt,))
        t.start()
        t.msg.connect(self.status_check_fail)
        try:
            self.mainWidget = self.widget_after_login(_id=data['id'], email=email)
            self.mainWidget.closed.connect(self.main_closed)
        except:
            debugger.exception("FATAL")
        self.close()
        self.mainWidget.show()

    def submit(self, _id, _pw):
        try:
            r = requests.post(LOGIN_URL, json={'username': _id,
                                               'password': hashlib.sha256(_pw.encode()).hexdigest(),
                                               'program': self.pid})
            data = r.json()
        except:
            QMessageBox.about(self, Msg.Title.LOGIN_FAILED, Msg.Content.SERVER_IS_CLOSED)
            return False

        if data['valid_id'] is False:
            QMessageBox.about(self, Msg.Title.LOGIN_FAILED, Msg.Content.WRONG_ID)
            return False
        elif data['expired']:
            QMessageBox.about(self, Msg.Title.LOGIN_FAILED, Msg.Content.EXPIRED_ID)
            return False
        elif data['duplicated_connection']:
            box = QMessageBox()
            box.setIcon(QMessageBox.Question)
            box.setWindowTitle('로그인 실패')
            box.setText('다른 네트워크에서 사용중인 ID 입니다.\n다른 네트워크의 연결을 끊고 로그인 하시겠습니까?')

            box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            button_yes = box.button(QMessageBox.Yes)
            button_yes.setText('확인')
            button_no = box.button(QMessageBox.No)
            button_no.setText('취소')
            res = box.exec_()
            if res == 16384:
                if self.update_ip(data, first_login=True) is False:
                    return False
            elif res == 65536:
                return False
        else:
            if self.update_ip(data) is False:
                return False

        if data['notice']:
            QMessageBox.about(self, '공지', "{}".format(data['notice']))

        if data['version'] != VERSION:
            QMessageBox.about(self,
                              '공지',
                              ("최신 버전이 아닙니다. 최신버전을 다운로드 해주세요.\n"
                               "현재 버전: {}\n최신 버전: {}").format(VERSION, data['version'])
                              )
            return False

        self.after_login(data, _id)

    def update_ip(self, data, first_login=False):
        try:
            r = requests.post(UPDATE_IP_URL, json={'id': data['id'],
                                         'first_login': first_login})
            data = r.json()
        except:
            QMessageBox.about(self, Msg.Title.LOGIN_FAILED, Msg.Content.SERVER_IS_CLOSED)
            return False

        return True

    def main_closed(self):
        self.show()

    def status_check_fail(self, msg):
        QMessageBox.about(self, Msg.Title.LOGIN_FAILED, msg)
        self.mainWidget.close()


class StatusCheck(QThread):
    msg = pyqtSignal(str)

    def __init__(self, user_id):
        super().__init__()
        self.user_id = user_id

    def run(self):
        self.msg.emit(check_status(self.user_id, evt))


if __name__ == '__main__':
    try:
        app = QApplication([])
        gui = LoginWidget(pid='SAIDiffTrader', widget_after_login=DiffTraderGUI)
        gui.show()
        app.exec_()
    except:
        debugger.exception("FATAL")
    finally:
        debugger.debug('Done')