from urllib.parse import urlparse
import banco
import logging

conn, cur = banco.conecta_banco()


def facebook_importa_empresas():
    sql = 'SELECT `idempresa`, `urlredesocial` from `empresaredessociais` WHERE `idredesocial` = 1'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']
        if url == '' or url is None:
            continue
        idempresa = result['idempresa']

        if 'http' not in url:
            url_rede = 'http://' + url
        else:
            url_rede = url

        username = urlparse(url_rede).path.replace('/', '')
        sql_result = 'SELECT `username` from `fb_perfis` WHERE `idempresa` = (%s) AND `idmarca` is null'
        cur.execute(sql_result, idempresa)
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO fb_perfis (`idempresa`, `username`) VALUES (%s, %s)'
            cur.execute(sql, (idempresa, username))
            conn.commit()
        else:
            if perfil['username'] == username:
                continue
            else:
                sql = 'INSERT INTO `fb_perfis` (`username`, `idempresa`) VALUES (%s, %s)'
                cur.execute(sql, (username, idempresa))
                conn.commit()


def facebook_importa_marcas():
    sql = 'select `marcaredessociais`.`idmarca`,`idempresa`,`urlredesocial` from `marcaredessociais`' \
          ' inner join `marcas` ON `marcaredessociais`.`idmarca` = `marcas`.`idmarca` where `idredesocial` = 1'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']
        idempresa = result['idempresa']
        idmarca = result['idmarca']

        if 'http' not in url:
            url_rede = 'http://' + url
        else:
            url_rede = url

        username = urlparse(url_rede).path.replace('/', '')
        sql_result = 'SELECT `username` from `fb_perfis` WHERE `idempresa` = (%s) AND `idmarca` = (%s)'
        cur.execute(sql_result, (idempresa, idmarca))
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO fb_perfis (`idempresa`, `idmarca`, `username`) VALUES (%s, %s, %s)'
            cur.execute(sql, (idempresa, idmarca, username))
            conn.commit()
        else:
            if perfil['username'] == username:
                continue
            else:
                sql = 'INSERT INTO `fb_perfis` (`username`, `idempresa`, `idmarca`) VALUES (%s, %s, %s)'
                cur.execute(sql, (username, idempresa, idmarca))
                conn.commit()


def twitter_importa_empresas():
    sql = 'SELECT `idempresa`, `urlredesocial` from `empresaredessociais` WHERE `idredesocial` = 2'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']
        if url == '' or url is None:
            continue
        idempresa = result['idempresa']

        if 'http' not in url:
            url_rede = 'http://' + url
        else:
            url_rede = url

        screenname = urlparse(url_rede).path.replace('/', '')
        sql_result = 'SELECT `screenname` from `tw_perfis` WHERE `idempresa` = (%s) AND `idmarca` is null'
        cur.execute(sql_result, idempresa)
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO tw_perfis (`idempresa`, `screenname`) VALUES (%s, %s)'
            cur.execute(sql, (idempresa, screenname))
            conn.commit()
        else:
            if perfil['screenname'] == screenname:
                continue
            else:
                sql = 'INSERT INTO `tw_perfis` (`screenname`, `idempresa`) VALUES (%s, %s)'
                cur.execute(sql, (screenname, idempresa))
                conn.commit()


def twitter_importa_marcas():
    sql = 'select `marcaredessociais`.`idmarca`,`idempresa`,`urlredesocial` from `marcaredessociais`' \
          ' inner join `marcas` ON `marcaredessociais`.`idmarca` = `marcas`.`idmarca` where `idredesocial` = 1'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']
        idempresa = result['idempresa']
        idmarca = result['idmarca']

        if 'http' not in url:
            url_rede = 'http://' + url
        else:
            url_rede = url

        screenname = urlparse(url_rede).path.replace('/', '')
        sql_result = 'SELECT `screenname` from `tw_perfis` WHERE `idempresa` = (%s) AND `idmarca` = (%s)'
        cur.execute(sql_result, (idempresa, idmarca))
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO tw_perfis (`idempresa`, `idmarca`, `screenname`) VALUES (%s, %s, %s)'
            cur.execute(sql, (idempresa, idmarca, screenname))
            conn.commit()
        else:
            if perfil['screenname'] == screenname:
                continue
            else:
                sql = 'INSERT INTO `tw_perfis` (`screenname`, `idempresa`, `idmarca`) VALUES (%s, %s, %s)'
                cur.execute(sql, (screenname, idempresa, idmarca))
                conn.commit()


def instagram_importa_empresas():
    sql = 'SELECT `idempresa`, `urlredesocial` from `empresaredessociais` WHERE `idredesocial` = 3'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']
        if url == '' or url is None:
            continue
        idempresa = result['idempresa']

        if 'http' not in url:
            url_rede = 'http://' + url
        else:
            url_rede = url

        username = urlparse(url_rede).path.replace('/', '')
        sql_result = 'SELECT `username` from `insta_perfis` WHERE `idempresa` = (%s) AND `idmarca` is null'
        cur.execute(sql_result, idempresa)
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO insta_perfis (`idempresa`, `username`) VALUES (%s, %s)'
            cur.execute(sql, (idempresa, username))
            conn.commit()
        else:
            if perfil['username'] == username:
                continue
            else:
                sql = 'INSERT INTO `insta_perfis` (`username`, `idempresa`) VALUES (%s, %s)'
                cur.execute(sql, (username, idempresa))
                conn.commit()


def instagram_importa_marcas():
    sql = 'select `marcaredessociais`.`idmarca`,`idempresa`,`urlredesocial` from `marcaredessociais`' \
          ' inner join `marcas` ON `marcaredessociais`.`idmarca` = `marcas`.`idmarca` where `idredesocial` = 3'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']
        idempresa = result['idempresa']
        idmarca = result['idmarca']

        if 'http' not in url:
            url_rede = 'http://' + url
        else:
            url_rede = url

        username = urlparse(url_rede).path.replace('/', '')
        sql_result = 'SELECT `username` from `insta_perfis` WHERE `idempresa` = (%s) AND `idmarca` = (%s)'
        cur.execute(sql_result, (idempresa, idmarca))
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO insta_perfis (`idempresa`, `idmarca`, `username`) VALUES (%s, %s, %s)'
            cur.execute(sql, (idempresa, idmarca, username))
            conn.commit()
        else:
            if perfil['username'] == username:
                continue
            else:
                sql = 'INSERT INTO `insta_perfis` (`username`, `idempresa`, `idmarca`) VALUES (%s, %s, %s)'
                cur.execute(sql, (username, idempresa, idmarca))
                conn.commit()


def youtube_importa_empresas():
    sql = 'SELECT `idempresa`, `urlredesocial` from `empresaredessociais` WHERE `idredesocial` = 4'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']

        if url == '' or url is None:
            continue
        if '?' in url:
            url = url[:url.index('?')]

        idempresa = result['idempresa']

        if '/user/' in url or '/c/' in url:
            idyoutube = None
            username = url[url.rfind('/')+1:]
        elif '/channel/' in url:
            username = None
            idyoutube = url[url.rfind('/')+1:]
        elif 'youtube.com/' in url:
            idyoutube = None
            username = url[url.rfind('/')+1:]
        else:
            logging.critical('URL %s não é válida', url)
            exit()

        sql_result = 'SELECT `idyoutube`, `username` from `yt_canais` WHERE `idempresa` = (%s) AND `idmarca` is null'
        cur.execute(sql_result, idempresa)
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO `yt_canais` (`idempresa`, `idyoutube`, `username`) VALUES (%s, %s, %s)'
            cur.execute(sql, (idempresa, idyoutube, username))
            conn.commit()
        else:
            if perfil['idyoutube'] == idyoutube or perfil['username'] == username:
                continue
            else:
                sql = 'INSERT INTO `yt_canais` (`idempresa`, `idyoutube`, `username`) VALUES (%s, %s, %s)'
                cur.execute(sql, (idempresa, idyoutube, username))
                conn.commit()


def youtube_importa_marcas():
    sql = 'select `marcaredessociais`.`idmarca`,`idempresa`,`urlredesocial` from `marcaredessociais`' \
          ' inner join `marcas` ON `marcaredessociais`.`idmarca` = `marcas`.`idmarca` where `idredesocial` = 4'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        url = result['urlredesocial']
        idempresa = result['idempresa']
        idmarca = result['idmarca']

        if 'http' not in url:
            url_rede = 'http://' + url
        else:
            url_rede = url

        username = urlparse(url_rede).path.replace('/', '')
        sql_result = 'SELECT `username` from `yt_canais` WHERE `idempresa` = (%s) AND `idmarca` = (%s)'
        cur.execute(sql_result, (idempresa, idmarca))
        perfil = cur.fetchone()

        if not perfil:
            sql = 'INSERT INTO yt_canais (`idempresa`, `idmarca`, `username`) VALUES (%s, %s, %s)'
            cur.execute(sql, (idempresa, idmarca, username))
            conn.commit()
        else:
            if perfil['username'] == username:
                continue
            else:
                sql = 'INSERT INTO `yt_canais` (`username`, `idempresa`, `idmarca`) VALUES (%s, %s, %s)'
                cur.execute(sql, (username, idempresa, idmarca))
                conn.commit()


facebook_importa_empresas()
#facebook_importa_marcas()
twitter_importa_empresas()
#twitter_importa_marcas()
instagram_importa_empresas()
#instagram_importa_marcas()
youtube_importa_empresas()
#youtube_importa_marcas()