import logging
import pymysql


def conecta_banco():
    try:
        # conn = pymysql.connect(host='172.16.0.85', user='sazeeng',
        #                       passwd='zeengdev', db='zeeng',
        #                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        conn = pymysql.connect(host='mysql-idc.plugar.com.br', user='andre.saldanha',
                               passwd='PskwUkj2&23$', db='zeeng',
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        # conn = pymysql.connect(host='187.103.103.11', user='andre',
        #                         passwd='Zeeng1234#', db='zeeng',
        #                         charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        cur = conn.cursor()
        return conn, cur
    except pymysql.err.Error as excpt:
        logging.critical('%s', excpt)
