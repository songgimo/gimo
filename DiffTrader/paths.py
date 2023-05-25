import os
import sys

from PyQt5 import uic

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


class LoginWidgets(object):
    LOGIN_WIDGET = uic.loadUiType(os.path.join(ROOT_DIR, 'static/guis/Login.ui'))[0]


class DialogWidgets(object):
    KEY_DIALOG_WIDGET = uic.loadUiType(os.path.join(ROOT_DIR, 'static/guis/SettingEncryptKeyDialog.ui'))[0]
    CONFIRM_DIALOG_WIDGET = uic.loadUiType(os.path.join(ROOT_DIR, 'static/guis/DifferentKeyInputDialog.ui'))[0]


class ProgramSettingWidgets(object):
    PROFIT_SETTING_WIDGET = uic.loadUiType(os.path.join(ROOT_DIR, 'static/guis/ProfitSettingWidget.ui'))[0]
    DIFF_TRADER_WIDGET = uic.loadUiType(os.path.join(ROOT_DIR, 'static/guis/main.ui'))[0]
