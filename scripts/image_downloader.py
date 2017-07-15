from safe_requests import safe_retrieve
import banco
import os

conn, cur = banco.conecta_banco()

path = '/media/Shared-Images/facebook/'
sql = "SELECT urlimagem, nomeimagem FROM fb_posts WHERE urlimagem is not null"

cur.execute(sql)
results = cur.fetchall()

for result in results:
    if not os.path.isfile(path + result['nomeimagem']):
        safe_retrieve(result['urlimagem'], path + result['nomeimagem'])


path = '/media/Shared-Images/twitter/'
sql = "SELECT urlimagem, nomeimagem FROM tw_posts WHERE urlimagem is not null"

cur.execute(sql)
results = cur.fetchall()

for result in results:
    if not os.path.isfile(path + result['nomeimagem']):
        safe_retrieve(result['urlimagem'], path + result['nomeimagem'])


path = '/media/Shared-Images/instagram/'
sql = "SELECT urlimagem, nomeimagem FROM insta_posts WHERE urlimagem is not null"

cur.execute(sql)
results = cur.fetchall()

for result in results:
    if not os.path.isfile(path + result['nomeimagem']):
        safe_retrieve(result['urlimagem'], path + result['nomeimagem'])


path = '/media/Shared-Images/youtube/'
sql = "SELECT urlimagem, nomeimagem FROM yt_videos WHERE urlimagem is not null"

cur.execute(sql)
results = cur.fetchall()

for result in results:
    if not os.path.isfile(path + result['nomeimagem']):
        safe_retrieve(result['urlimagem'], path + result['nomeimagem'])
