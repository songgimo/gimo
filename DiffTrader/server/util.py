from DiffTrader.server.settings import SqlInfo as info
from mysql.connector import pooling

from Util.pyinstaller_patch import debugger

CONNECTION_POOL = pooling.MySQLConnectionPool(
    pool_name='server_pool',
    pool_size=3,
    **info.POOL_CONFIG
)


def execute_db(query, value=None, custom_cursor=None):
    con = CONNECTION_POOL.get_connection()

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
        con = CONNECTION_POOL.get_connection()
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
