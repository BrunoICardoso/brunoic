import logging
import pymysql


def conecta_banco():
    try:
        conn = pymysql.connect(host='localhost', user='andre',
                               passwd='Zeeng1234#', db='zeeng_homolog',
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        cur = conn.cursor()
        return conn, cur
    except pymysql.err.Error as excpt:
        logging.critical('%s', excpt)