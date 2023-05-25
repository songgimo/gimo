from PyQt5 import (QtWidgets)
from Crypto.Cipher import AES

import os
import json
import hashlib


def save(exchange, password, **kwargs):
    """
        Args:
            exchange: exchange string.
            password: password for encrypt the key and secret.
        Return:
            True if key is saved successful else False
    """
    if os.path.exists('Settings'):
        data = load(password)
        if not data:
            return False
    else:
        data = {'key': password}

    with open('Settings', 'wb') as sf:
        hkey = hashlib.sha256(password.encode()).digest()
        aes = AES.new(hkey, AES.MODE_EAX, hkey)
        ec = exchange.lower()
        data[ec] = {}
        data[ec].update(kwargs)

        jdata = json.dumps(data)
        enc = aes.encrypt(jdata.encode())

        sf.write(enc)
    return True


def load(password):
    """
        Args:
            password: password for encrypt the key and secret.
        Return:
            True if key is saved successful else False
    """
    if not os.path.exists('Settings'):
        return {}
    with open('Settings', 'rb') as sf:
        edata = sf.read()
        hkey = hashlib.sha256(password.encode()).digest()
        aes = AES.new(hkey, AES.MODE_EAX, hkey)
        jdata = aes.decrypt(edata)
        try:
            data = json.loads(jdata.decode())
        except:
            return False

    return data


def base_item_setter(row, table, data_set):
    """
        Args:
            row: insert to this row
            table: table object
            data_set: list, data that is inserted to table
    """
    for num, each in enumerate(data_set):
        item = table.item(row, num)
        if not item:
            item = QtWidgets.QTableWidgetItem(str(each))
            table.setItem(row, num, item)
        else:
            item.setText(str(each))


def number_type_converter(to_type, value):
    """
        Args:
            to_type: Convert value to this parameter
            value: value, It is must be int type.
    """
    try:
        if not value:
            return int()
        else:
            if not isinstance(value, to_type):
                return to_type(value)
            else:
                return value
    except ValueError:
        return int()
