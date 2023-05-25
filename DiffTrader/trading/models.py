from PyQt5.QtCore import (Qt, QAbstractTableModel,
                          QVariant, QModelIndex)


class TradeTableModel(QAbstractTableModel):
    def __init__(self, _header, _data, _id=[]):
        super(TradeTableModel, self).__init__()
        self._data = _data
        self._header = _header
        self._id = _id

    def rowCount(self, parent=None, *args, **kwargs):
        return len(self._data)

    def columnCount(self, parent=None, *args, **kwargs):
        return len(self._header)

    def headerData(self, p_int, Qt_Orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and Qt_Orientation == Qt.Horizontal:
            return self._header[p_int]
        elif role == Qt.DisplayRole and Qt_Orientation == Qt.Vertical:
            return "{}".format(p_int)
        else:
            return QVariant()

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.DisplayRole or role == Qt.EditRole:
            return self._data[index.row()][index.column()]
        elif role == Qt.TextAlignmentRole:
            return Qt.AlignRight | Qt.AlignCenter
        else:
            return QVariant()

    def setData(self, index, change, role=Qt.EditRole):
        self._data[index.row()][index.column()] = change
        return True

    def insertRow(self, p_int, parent=None, *args, **kwargs):
        self.beginInsertRows(QModelIndex(), p_int, p_int)
        self._data.append(list(*args))
        self.endInsertRows()
        return True

    def removeRow(self, p_int, parent=None, *args, **kwargs):
        self.beginRemoveRows(QModelIndex(), p_int, p_int)
        self._data.pop(p_int)
        self.endRemoveRows()
        return True

    def flags(self, index):
        return Qt.ItemIsEditable | Qt.ItemIsEnabled | Qt.ItemIsSelectable
