import datetime
import logging
import time

import banco
from safe_requests import get_request_and_jsonize
from safe_requests import safe_retrieve

# https://api.instagram.com/oauth/authorize/?client_id=5a3704f31cff47b69db5305b64385654&redirect_uri=http://www.zeeng.com.br&response_type=token&scope=public_content
access_token = "230255628.5a3704f.518157c10cb94356b12725178d420474"
conn, cur = banco.conecta_banco()
DIAS = 90
pathinsta = '/media/Shared-Images/instagram/'


def instagram_get_perfil(idperfil, username):
    logging.info('Iniciando captura do perfil %s', username)
    url = ('https://api.instagram.com/v1/users/search/?q=' + username + '&count=10&access_token=' + access_token)

    search = get_request_and_jsonize(url)

    if len(search['data']) == 0:
        logging.error('O perfil %s não existe.', username)
        return

    for perfil in search['data']:
        if perfil['username'] == username:
            idinstagram = perfil['id']
            nome = perfil['full_name']
            urlimagem = perfil['profile_picture']
            if '.png' in urlimagem:
                nomeimagem = idinstagram + '.png'
            else:
                nomeimagem = idinstagram + '.jpg'

            # TODO safe_retrieve(urlimagem, nomeimagem)

            url = ('https://api.instagram.com/v1/users/' + idinstagram + '?access_token=' + access_token)

            search = get_request_and_jsonize(url)
            perfil = search['data']
            sobre = perfil['bio']
            website = perfil['website']

            sql = 'UPDATE `insta_perfis` SET `idinstagram` = (%s), `nome` = (%s), `sobre` = (%s), `website` = (%s), ' \
                  '`urlimagem` = (%s), `nomeimagem` = (%s) WHERE `idperfil` = (%s)'

            cur.execute(sql, (idinstagram, nome, sobre, website, urlimagem, nomeimagem, idperfil))
            conn.commit()

            return idinstagram
        else:
            continue

    logging.critical('O perfil %s não existe.', username)
    return


def instagram_get_perfil_stats(idperfil, idinstagram):
    hoje = datetime.date.today()
    sql = 'SELECT * FROM insta_perfis_stats WHERE `idperfil` = (%s) AND DATE(datahora) = (%s)'
    cur.execute(sql, (idperfil, hoje))
    existe = cur.fetchone()
    if existe:
        logging.info('Estatísticas já capturadas hoje.')
        return

    logging.info('Iniciando a captura de estatísticas do perfil de id %d.', idperfil)
    url = ('https://api.instagram.com/v1/users/' + idinstagram + '?access_token=' + access_token)

    search = get_request_and_jsonize(url)
    perfil = search['data']['counts']

    datahora = datetime.datetime.utcnow()
    qtdmidias = perfil['media']
    qtdseguidores = perfil['followed_by']
    qtdseguidos = perfil['follows']

    sql = 'INSERT INTO `insta_perfis_stats` (`idperfil`, `datahora`, `qtdmidias`, `qtdseguidores`, `qtdseguidos`)' \
          'VALUES (%s, %s, %s, %s, %s)'

    cur.execute(sql, (idperfil, datahora, qtdmidias, qtdseguidores, qtdseguidos))
    conn.commit()


def instagram_get_posts(idperfil, userid, datahoraultimopost):
    if datahoraultimopost is None:
        logging.info('Iniciando captura de posts do perfil %d', idperfil)
    else:
        logging.info('Iniciando captura de novos post do perfil %d', idperfil)

    url = ('https://api.instagram.com/v1/users/' + userid + '/media/recent/?count=33&access_token=' + access_token)
    search = get_request_and_jsonize(url)

    if len(search['data']) == 0:
        logging.info('Perfil %d não possui nenhum post', idperfil)
        return

    posts = search['data']
    novadatahoraultimopost = datetime.datetime.fromtimestamp(int(search['data'][0]['created_time']))
    if datahoraultimopost:
        if novadatahoraultimopost <= datahoraultimopost:
            logging.info('Nenhum post novo na página de id %d', idperfil)
            return

    sql = 'UPDATE `insta_perfis` SET `datahoraultimopost` = (%s) WHERE `idperfil` = (%s)'
    try:
        cur.execute(sql, (novadatahoraultimopost, idperfil))
        conn.commit()
    except Exception as excpt:
        logging.critical('%s', excpt)
        exit()

    sql = 'INSERT INTO `insta_posts` (`idperfil`, `idinstagram`, `postagem`, `datahora`, ' \
          '`qtdcomentarios`, `qtdcurtidas`, `tipo`, `link`, `urlimagem`, `nomeimagem`)' \
          ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '

    count = 0
    while True:
        for registro in posts:
            datahora = datetime.datetime.fromtimestamp(int(registro['created_time']))
            if datahoraultimopost:
                if datahora <= datahoraultimopost:
                    conn.commit()
                    logging.info('Capturados os últimos %d posts do perfil de id %d', count, idperfil)
                    return
            idinstagram = registro['id']
            if registro['caption']:
                postagem = registro['caption']['text']
            else:
                postagem = None
            qtdcomentarios = registro['comments']['count']
            qtdcurtidas = registro['likes']['count']
            tipo = registro['type']
            link = registro['link']
            urlimagem = registro['images']['standard_resolution']['url']
            if '.png' in urlimagem:
                nomeimagem = idinstagram + '.png'
            else:
                nomeimagem = idinstagram + '.jpg'

            # safe_retrieve(urlimagem, pathinsta + nomeimagem)
            try:
                cur.execute(sql, (idperfil, idinstagram, postagem, datahora, qtdcomentarios, qtdcurtidas,
                                  tipo, link, urlimagem, nomeimagem))
                count += 1
            except Exception as excpt:
                logging.error('%s', excpt)

        conn.commit()

        logging.info('Capturados os últimos %d posts do perfil', count)

        if 'pagination' in search and 'next_url' in search['pagination']:
            search = get_request_and_jsonize(search['pagination']['next_url'])
            posts = search['data']
            if len(posts) == 0:
                break
        else:
            break


def instagram_get_comentarios_page(idperfil, init):
    if init is True:
        logging.info('Iniciando captura de comentários dos posts do perfil de id %d', idperfil)
    else:
        logging.info('Iniciando a captura de novos comentários dos posts de até 90 dias do perfil de id %d', idperfil)

    since = datetime.date.today() - datetime.timedelta(days=DIAS)
    if init is True:
        sql = 'SELECT `idpost`, `idinstagram`, `datahoraultimocomentario` FROM `insta_posts`' \
              'WHERE `idperfil` = (%s)'
        cur.execute(sql, idperfil)
    else:
        sql = 'SELECT `idpost`, `idinstagram`, `datahoraultimocomentario` FROM `insta_posts`' \
              'WHERE `idperfil` = (%s) AND `datahora` >= (%s)'
        cur.execute(sql, (idperfil, since))

    results = cur.fetchall()
    for post in results:
        instagram_get_comentarios_post(idpost=post['idpost'], idpost_instagram=post['idinstagram'],
                                       datahoraultimocomentario=post['datahoraultimocomentario'])


def instagram_get_comentarios_post(idpost, idpost_instagram, datahoraultimocomentario):
    url = ('https://api.instagram.com/v1/media/' + idpost_instagram + '/comments?access_token=' + access_token)
    search = get_request_and_jsonize(url)
    
    if 'data' not in search:
        logging.error('Erro na captura de comentários do post %d', idpost)
        return

    comentarios = search['data']

    if len(comentarios) == 0:
        logging.info('Nenhum comentário no post de id %d.', idpost)
        return

    novadatahoraultimocomentario = datetime.datetime.fromtimestamp(int(comentarios[-1]['created_time']))
    if datahoraultimocomentario:
        if novadatahoraultimocomentario <= datahoraultimocomentario:
            logging.info('Nenhum comentário novo no post de id %d', idpost)
            return

    sql = 'UPDATE `insta_posts` SET `datahoraultimocomentario` = (%s) WHERE idpost = (%s)'
    cur.execute(sql, (novadatahoraultimocomentario, idpost))
    conn.commit()

    sql = 'INSERT INTO `insta_comentarios`' \
          '(`idpost`, `idinstagram`, `postagem`, `datahora`, `idusuario`, `nomeusuario`)' \
          'VALUES (%s, %s, %s, %s, %s, %s) '

    count = 0
    for registro in reversed(comentarios):
        datahora = datetime.datetime.fromtimestamp(int(registro['created_time']))
        if datahoraultimocomentario:
            if datahora <= datahoraultimocomentario:
                conn.commit()
                logging.info('Capturados os últimos %d comentários do post %d', count, idpost)
                return
        idinstagram = registro['id']
        postagem = registro['text']
        idusuario = registro['from']['id']
        nomeusuario = registro['from']['username']

        try:
            cur.execute(sql, (idpost, idinstagram, postagem, datahora, idusuario, nomeusuario))
            count += 1
        except Exception as excpt:
            logging.error('%s', excpt)

    conn.commit()

    logging.info('Capturados os últimos %d comentários do post de id %d', count, idpost)


def instagram_update_perfil(idperfil, idinstagram):
    logging.info('Iniciando atualização do perfil de id %d', idperfil)

    url = ('https://api.instagram.com/v1/users/' + idinstagram + '?access_token=' + access_token)
    perfil = get_request_and_jsonize(url)['data']

    nome = perfil['full_name']
    username = perfil['username']
    urlimagem = perfil['profile_picture']
    # nomefoto = username + ".jpg"
    # urllib.request.urlretrieve(urlfoto, nomefoto)

    sobre = perfil['bio']
    website = perfil['website']
    ultimaatualizacao = datetime.date.today()

    sql = 'UPDATE `insta_perfis` SET `nome` = (%s), `username` = (%s), `urlimagem` = (%s),' \
          '`sobre`= (%s), `website` = (%s), `ultimaatualizacao` = (%s) WHERE idperfil = (%s)'

    cur.execute(sql, (nome, username, urlimagem, sobre, website, ultimaatualizacao, idperfil))
    conn.commit()


def instagram_update_posts(idperfil, idinstagram):
    logging.info('Iniciando atualização de posts do perfil de id %d', idperfil)

    url = ('https://api.instagram.com/v1/users/' + idinstagram + '/media/recent/?count=33&access_token=' + access_token)
    search = get_request_and_jsonize(url)
    posts = search['data']
    since = datetime.date.today() - datetime.timedelta(days=DIAS)

    sql = 'UPDATE `insta_posts` SET `qtdcomentarios` = (%s), `qtdcurtidas` = (%s) WHERE `idinstagram` = (%s)'

    count = 0
    while True:
        for registro in posts:
            idinstagram = registro['id']
            datahora = datetime.datetime.fromtimestamp(int(registro['created_time']))
            if datahora.date() < since:
                conn.commit()
                logging.info('Atualizados os últimos %d posts', count)
                return

            qtdcomentarios = registro['comments']['count']
            qtdcurtidas = registro['likes']['count']

            count += 1

            cur.execute(sql, (qtdcomentarios, qtdcurtidas, idinstagram))

        conn.commit()

        if 'pagination' in search and 'next_url' in search['pagination']:
            search = get_request_and_jsonize(search['pagination']['next_url'])
            posts = search['data']
            if len(posts) == 0:
                break
        else:
            break


def instagram_update():
    sql = 'SELECT * FROM `insta_perfis` WHERE `ultimaatualizacao` is not null'
    cur.execute(sql)
    perfis = cur.fetchall()

    for perfil in perfis:
        idperfil = perfil['idperfil']
        idinstagram = perfil['idinstagram']
        datahoraultimopost = perfil['datahoraultimopost']
        ultimaatualizacao = perfil['ultimaatualizacao']

        datanow = datetime.date.today()

        if (datanow - ultimaatualizacao).days > 90:
            instagram_update_perfil(idperfil, idinstagram)
            ultimaatualizacao = datanow
            sql = 'UPDATE insta_perfis SET `ultimaatualizacao` = (%s) WHERE `idperfil` = (%s)'
            cur.execute(sql, (ultimaatualizacao, idperfil))
            conn.commit()

        instagram_get_perfil_stats(idperfil, idinstagram)
        instagram_get_posts(idperfil, idinstagram, datahoraultimopost)
        instagram_get_comentarios_page(idperfil, init=False)
        instagram_update_posts(idperfil, idinstagram)


def instagram_init():
    sql = 'SELECT `idperfil`, `username` FROM `insta_perfis` WHERE `ultimaatualizacao` is null'
    cur.execute(sql)
    results = cur.fetchall()
    for result in results:
        idperfil = result['idperfil']
        username = result['username']

        idinstagram = instagram_get_perfil(idperfil, username)
        instagram_get_perfil_stats(idperfil, idinstagram)
        instagram_get_posts(idperfil, idinstagram, None)
        instagram_get_comentarios_page(idperfil, init=True)

        ultimaatualizacao = datetime.date.today()
        sql = 'UPDATE insta_perfis SET `ultimaatualizacao` = (%s) WHERE `idperfil` = (%s)'
        cur.execute(sql, (ultimaatualizacao, idperfil))
        conn.commit()


if __name__ == "__main__":
    import sys
    start_time = time.time()
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    if sys.argv[1] == 'init':
        instagram_init()
    elif sys.argv[1] == 'update':
        instagram_update()
    else:
        logging.info('Comando: python ./instagram_agente.py [init|update]')
        exit()
    logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
    logging.info("Script executado com sucesso!")

cur.close()
conn.close()
