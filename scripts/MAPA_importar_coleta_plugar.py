import logging
import pymysql

connZeeng = pymysql.connect(host='187.103.103.11', user='andre',
                                passwd='Zeeng1234#', db='zeeng',
                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
curZeeng = connZeeng.cursor()

connPlugar = pymysql.connect(host='mysql-idc.plugar.com.br', user='andre.saldanha',
                       passwd='PskwUkj2&23$', db='zeeng',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

curPlugar = connPlugar.cursor()


sql ='SELECT * FROM mapa_dadoscaptura where idmapacaptura = (%s)'

curPlugar.execute(sql)
results = curPlugar.fetchall()










