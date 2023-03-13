from KiwoomHighChart.config import SqlInfo as info
import pymysql

from Util.pyinstaller_patch import debugger
import datetime

con = pymysql.connect(host=info.HOST, user=info.USER, password=info.PASSWORD, charset='utf8', db=info.DATABASE)
# con = None


def execute_db(query, value=None, custom_cursor=None):
    global con
    if not con.open:
        con = pymysql.connect(host=info.HOST, user=info.USER, password=info.PASSWORD, charset='utf8', db=info.DATABASE)

    with con.cursor(custom_cursor) as cursor:
        if value:
            cursor.execute(query, value)
        else:
            cursor.execute(query)

        debugger.info(cursor._last_executed)
        data = cursor.fetchall()

    con.commit()

    return data


def execute_db_many(query, value_list, *args):
    try:
        global con
        if not con.open:
            con = pymysql.connect(host=info.HOST, user=info.USER, password=info.PASSWORD, charset='utf8',
                                  db=info.DATABASE)

        with con.cursor() as cursor:
            if value_list:
                cursor.executemany(query, [tuple(list(args) + each) for each in value_list])
            else:
                raise
            debugger.info(cursor._last_executed)
            data = cursor.fetchall()
        con.commit()
        return data
    except Exception:
        debugger.info(query)
        raise
