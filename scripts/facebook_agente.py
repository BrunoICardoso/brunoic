import datetime
import logging
import time

import banco
import pymysql
from safe_requests import get_request_and_jsonize
from safe_requests import safe_retrieve

conn, cur = banco.conecta_banco()

graph_url = 'https://graph.facebook.com/'
api_version = 'v2.9/'
access_token = 'EAAZASk8Jo4KIBAG68kIPDKG3JKTZC7ZAZBMD5ZAoktBMzOoQL8aJlGkZBkBZCweOeqxZBS5SbpJfeDWdhlVZAh1umn8w42ZB3ug4BE6TC3h6PFCeveYzhhQyl0BgGJAq2ec4dpWiZAuW67HfdDYsYAQl3mVuB0SrLNRea4ZD'
MAXCOMMENTLEN = 8000
MAXPOSTLEN = 10000
DIAS = 90
MAXURLIMG = 3000
pathface = '/media/Shared-Images/facebook/'
pathempresa = '/media/Shared-Images/empresas/'


def facebook_get_page(idempresa, idperfil, idorusername):
    logging.info('Iniciando captura da página: %s', idorusername)

    url = (graph_url + api_version + idorusername + '/?access_token=' + access_token +
           '&fields=id,about,username,description,name,link,website,picture.type(large)')

    page = get_request_and_jsonize(url)

    if 'error' in page:
        logging.critical('%s', page['error']['message'])
        raise Exception

    idfacebook = page['id']
    nome = page['name']
    sobre = page['about'] if 'about' in page else None
    descricao = page.get('description')
    website = page['website'] if 'website' in page else None
    urlimagem = page['picture']['data']['url']
    if '.png' in urlimagem:
        nomeimagem = idfacebook + '.png'
        empresaimagem = str(idempresa) + '.png'
    else:
        empresaimagem = str(idempresa) + '.jpg'
        nomeimagem = idfacebook + '.jpg'

    # safe_retrieve(urlimagem, pathface + nomeimagem)
    safe_retrieve(urlimagem, pathempresa + empresaimagem)

    sql = 'UPDATE empresas SET `imagem` = (%s) WHERE `idempresa` = (%s)'

    try:
        cur.execute(sql, (empresaimagem, idempresa))
        conn.commit()
    except Exception as excpt:
        logging.error('%s', excpt)

    link = page['link']

    sql = "UPDATE `fb_perfis` SET `idfacebook` = (%s), `nome` = (%s), `sobre` = (%s), `descricao` = (%s)," \
          "`urlimagem` = (%s), `nomeimagem` = (%s), `website` = (%s), `link` = (%s)" \
          "WHERE `idperfil` = (%s)"
    try:
        cur.execute(sql, (idfacebook, nome, sobre, descricao, urlimagem,
                          nomeimagem, website, link, idperfil))
        conn.commit()
    except Exception as excpt:
        logging.critical('%s', excpt)
        raise Exception

    return idfacebook


def facebook_get_page_stats(idperfil, idfacebook):
    logging.info('Iniciando captura das estatísticas da página de id %d', idperfil)

    hoje = datetime.date.today()
    sql = 'SELECT * FROM fb_perfis_stats WHERE `idperfil` = (%s) AND DATE(datahora) = (%s)'
    cur.execute(sql, (idperfil, hoje))
    existe = cur.fetchone()
    if existe:
        logging.info('Estatísticas já capturadas hoje.')
        return

    url = (graph_url + api_version + idfacebook + '/?access_token=' + access_token
           + '&fields=fan_count,overall_star_rating,rating_count,'
             'talking_about_count')

    page = get_request_and_jsonize(url)

    if 'error' in page:
        logging.critical('%s', page['error']['message'])
        raise Exception

    datahora = datetime.datetime.utcnow()
    qtdlikes = page['fan_count']
    qtdpessoasfalando = page['talking_about_count']
    qtdestrelas = page['overall_star_rating'] if 'overall_star_rating' in page else 0
    qtdavaliacoes = page['rating_count']

    sql = "INSERT INTO `fb_perfis_stats` (`idperfil`, `datahora`, `qtdlikes`, `qtdestrelas`," \
          "`qtdavaliacoes`, `qtdpessoasfalando`) VALUES (%s, %s, %s, %s, %s, %s)"

    try:
        cur.execute(sql, (idperfil, datahora, qtdlikes, qtdestrelas, qtdavaliacoes, qtdpessoasfalando))
        conn.commit()
    except Exception as excpt:
        logging.critical('%s', excpt)
        raise Exception


def facebook_get_posts(idperfil, idfacebook, datahoraultimopost):
    url = (graph_url + api_version + idfacebook +
           '/posts?limit=100&fields=message,shares,created_time,type,link,'
           'reactions.limit(0).summary(total_count),'
           'reactions.type(LOVE).limit(0).summary(total_count).as(reactions_love),'
           'reactions.type(WOW).limit(0).summary(total_count).as(reactions_wow),'
           'reactions.type(HAHA).limit(0).summary(total_count).as(reactions_haha),'
           'reactions.type(LIKE).limit(0).summary(total_count).as(reactions_like),'
           'reactions.type(SAD).limit(0).summary(total_count).as(reactions_sad),'
           'reactions.type(ANGRY).limit(0).summary(total_count).as(reactions_angry),'
           'reactions.type(THANKFUL).limit(0).summary(total_count).as(reactions_thankful).summary(total_count),'
           'full_picture,permalink_url,comments.limit(0).summary(total_count).filter(stream)' +
           '&access_token=' + access_token)

    search = get_request_and_jsonize(url)

    if 'error' in search:
        logging.error('%s', search['error']['message'])
        return

    posts = search['data']

    if len(posts) == 0:
        logging.info('Nenhum post na página de id %d', idperfil)
        return

    novadatahoraultimopost = datetime.datetime.strptime(posts[0]['created_time'], '%Y-%m-%dT%H:%M:%S+0000')

    if datahoraultimopost:
        if novadatahoraultimopost <= datahoraultimopost:
            logging.info('Nenhum post novo na página de id %d', idperfil)
            return

    sql = 'UPDATE `fb_perfis` SET `datahoraultimopost` = (%s) WHERE `idperfil` = (%s)'
    try:
        cur.execute(sql, (novadatahoraultimopost, idperfil))
        conn.commit()
    except Exception as excpt:
        logging.critical('%s', excpt)
        raise Exception

    sql = "INSERT INTO `fb_posts` " \
          "(`idperfil`, `idfacebook`, `postagem`, `datahora`, `compartilhamentos`," \
          "`reacoes`, `reacoes_like`, `reacoes_haha`, `reacoes_wow`, `reacoes_sad`, `reacoes_love`, `reacoes_angry`, " \
          "`reacoes_thankful`, `comentarios`, `tipo`, `urlimagem`, `nomeimagem`, `link`) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    count = 0
    while True:
        for registro in posts:
            datahora = datetime.datetime.strptime(registro['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
            if datahoraultimopost:
                if datahora <= datahoraultimopost:
                    conn.commit()
                    logging.info('Capturados os últimos %d posts da página de id %d', count, idperfil)
                    return
            idfacebook = registro['id']
            if 'message' in registro:
                postagem = registro['message'] if len(registro['message']) <= MAXPOSTLEN \
                    else registro['message'][:MAXPOSTLEN]
            else:
                postagem = None
            compartilhamentos = registro['shares']['count'] if 'shares' in registro else 0

            reacoes = registro['reactions']['summary']['total_count'] if 'reactions' in registro else 0
            reacoes_like = registro['reactions_like']['summary']['total_count'] if 'reactions_like' in registro else 0
            reacoes_haha = registro['reactions_haha']['summary']['total_count'] if 'reactions_haha' in registro else 0
            reacoes_wow = registro['reactions_wow']['summary']['total_count'] if 'reactions_wow' in registro else 0
            reacoes_sad = registro['reactions_sad']['summary']['total_count'] if 'reactions_sad' in registro else 0
            reacoes_love = registro['reactions_love']['summary']['total_count'] if 'reactions_love' in registro else 0
            reacoes_angry = registro['reactions_angry']['summary']['total_count'] if 'reactions_angry' in registro else 0
            reacoes_thankful = registro['reactions_thankful']['summary']['total_count'] if 'reactions_thankful' in registro else 0

            if 'comments' in registro and 'total_count' in registro['comments']['summary']:
                comentarios = registro['comments']['summary']['total_count']
            else:
                comentarios = 0
            tipo = registro['type']

            urlimagem = None
            nomeimagem = None
            if 'full_picture' in registro:
                urlimagem = registro['full_picture'] if len(registro['full_picture']) <= MAXURLIMG \
                    else registro['full_picture'][:MAXURLIMG]
                if '.png' in urlimagem:
                    nomeimagem = idfacebook + '.png'
                else:
                    nomeimagem = idfacebook + '.jpg'

                # safe_retrieve(urlimagem, pathface + nomeimagem)

            link = registro['permalink_url']

            try:
                cur.execute(sql, (idperfil, idfacebook, postagem, datahora, compartilhamentos,
                                  reacoes, reacoes_like, reacoes_haha, reacoes_wow, reacoes_sad, reacoes_love,
                                  reacoes_angry, reacoes_thankful, comentarios, tipo, urlimagem, nomeimagem, link))
                count += 1
            except pymysql.err.IntegrityError as excpt:
                logging.error('%s', excpt)

        conn.commit()

        logging.info('Últimos %d posts da página de id %d capturados', count, idperfil)

        if 'paging' in search:
            if 'next' in search['paging']:
                search = get_request_and_jsonize(search['paging']['next'])
            else:
                break
            if 'data' in search:
                posts = search['data']
            else:
                break
        else:
            break

        if len(posts) == 0:
            break

    logging.info('Todos os %d posts capturados. Saindo da função de captura de posts.', count)


def facebook_get_comments_page(idperfil, init):
    logging.info('Iniciando captura de comentários dos posts da página de id %d', idperfil)

    since = datetime.date.today() - datetime.timedelta(days=DIAS)
    if init is True:
        sql = 'SELECT `idpost`, `idfacebook`, `datahoraultimocomentario` FROM `fb_posts`' \
              'WHERE `idperfil` = (%s) AND comentarios > 0'
        cur.execute(sql, idperfil)
    else:
        sql = 'SELECT `idpost`, `idfacebook`, `datahoraultimocomentario` FROM `fb_posts`' \
              'WHERE `idperfil` = (%s) AND `datahora` >= (%s) AND comentarios > 0'
        cur.execute(sql, (idperfil, since))

    results = cur.fetchall()

    for post in results:
        facebook_get_comments_post(idpost=post['idpost'], idfacebook=post['idfacebook'],
                                   datahoraultimocomentario=post['datahoraultimocomentario'])


def facebook_get_comments_post(idpost, idfacebook, datahoraultimocomentario):
    url = (graph_url + api_version + idfacebook +
           "/comments?limit=700&order=reverse_chronological&fields=message,"
           "created_time,id,"
           'reactions.limit(0).summary(total_count),'
           'reactions.type(LOVE).limit(0).summary(total_count).as(reactions_love),'
           'reactions.type(WOW).limit(0).summary(total_count).as(reactions_wow),'
           'reactions.type(HAHA).limit(0).summary(total_count).as(reactions_haha),'
           'reactions.type(LIKE).limit(0).summary(total_count).as(reactions_like),'
           'reactions.type(SAD).limit(0).summary(total_count).as(reactions_sad),'
           'reactions.type(ANGRY).limit(0).summary(total_count).as(reactions_angry),'
           'reactions.type(THANKFUL).limit(0).summary(total_count).as(reactions_thankful).summary(total_count),'
           "comment_count,from,attachment,parent{id}&filter=stream" +
           '&access_token=' + access_token)

    status = get_request_and_jsonize(url)

    if 'error' in status:
        logging.error('%s', status['error']['message'])
        return

    comments = status['data']

    if len(comments) > 0:
        novadatahoraultimocomentario = datetime.datetime.strptime(comments[0]['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
    else:
        logging.info('Nenhum comentário no post de id %d', idpost)
        return
    if datahoraultimocomentario:
        if novadatahoraultimocomentario <= datahoraultimocomentario:
            logging.info('Nenhum comentário novo no post de id %d', idpost)
            return

    sql = 'UPDATE `fb_posts` SET `datahoraultimocomentario` = (%s) WHERE idpost = (%s)'
    cur.execute(sql, (novadatahoraultimocomentario, idpost))
    conn.commit()

    sql = "INSERT INTO `fb_comentarios` (`idfacebook`, `idpost`, `postagem`, `datahora`, " \
          "`curtidas`, `reacoes_like`, `reacoes_haha`, `reacoes_wow`, `reacoes_sad`, `reacoes_love`, `reacoes_angry`, " \
          "`reacoes_thankful`, `respostas`, `idcomentarioresposta`, `urlimagem`, `nomeimagem`, `nomeusuario`, `idusuario`)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    count = 0
    while True:
        for comment in comments:
            datahora = datetime.datetime.strptime(comment['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
            if datahoraultimocomentario:
                if datahora <= datahoraultimocomentario:
                    conn.commit()
                    logging.info('Capturados os últimos %d comentários do post %d', count, idpost)
                    return
            tamanhopostagem = len(comment['message'])
            postagem = comment['message'] if tamanhopostagem < MAXCOMMENTLEN else comment['message'][:MAXCOMMENTLEN]
            idfacebook = comment['id']
            curtidas = comment['reactions']['summary']['total_count'] if 'reactions' in comment else 0
            reacoes_like = comment['reactions_like']['summary']['total_count'] if 'reactions_like' in comment else 0
            reacoes_haha = comment['reactions_haha']['summary']['total_count'] if 'reactions_haha' in comment else 0
            reacoes_wow = comment['reactions_wow']['summary']['total_count'] if 'reactions_wow' in comment else 0
            reacoes_sad = comment['reactions_sad']['summary']['total_count'] if 'reactions_sad' in comment else 0
            reacoes_love = comment['reactions_love']['summary']['total_count'] if 'reactions_love' in comment else 0
            reacoes_angry = comment['reactions_angry']['summary']['total_count'] if 'reactions_angry' in comment else 0
            reacoes_thankful = comment['reactions_thankful']['summary']['total_count'] if 'reactions_thankful' in comment else 0

            if 'parent' in comment:
                respostas = None
                idcomentarioresposta = comment['parent']['id']
            else:
                respostas = comment['comment_count']
                idcomentarioresposta = None

            urlimagem = None
            nomeimagem = None
            if 'attachment' in comment and 'media' in comment['attachment']:
                urlimagem = comment['attachment']['media']['image']['src']
                if '.png' in urlimagem:
                    nomeimagem = idfacebook + '.png'
                else:
                    nomeimagem = idfacebook + '.jpg'

            nomeusario = None
            idusuario = None
            if 'from' in comment:
                nomeusuario = comment['from']['name'] if 'name' in comment['from'] else None
                idusuario = comment['from']['id']

            try:
                cur.execute(sql, (idfacebook, idpost, postagem, datahora, curtidas, reacoes_like, reacoes_haha,
                                  reacoes_wow, reacoes_sad, reacoes_love, reacoes_angry, reacoes_thankful, respostas,
                                  idcomentarioresposta, urlimagem, nomeimagem, nomeusuario, idusuario))
                count += 1
            except Exception as excpt:
                logging.error('%s', excpt)

        conn.commit()

        if 'next' in status['paging']:
            status = get_request_and_jsonize(status['paging']['next'])
            if 'error' in status:
                logging.error('%s', status['error']['message'])
                return
            if 'data' in status:
                comments = status['data']
            else:
                break
        else:
            break

    logging.info('Capturados todos os %d comentários do post de id %d', count, idpost)


def facebook_update_posts(idfaceperfil):
    logging.info("Iniciando função de atualização de posts com até 90 dias")
    since = datetime.date.today() - datetime.timedelta(days=DIAS)

    url = (graph_url + api_version + idfaceperfil +
           '/posts?limit=100&fields=shares,'
           'reactions.limit(0).summary(total_count),'
           'reactions.type(LOVE).limit(0).summary(total_count).as(reactions_love),'
           'reactions.type(WOW).limit(0).summary(total_count).as(reactions_wow),'
           'reactions.type(HAHA).limit(0).summary(total_count).as(reactions_haha),'
           'reactions.type(LIKE).limit(0).summary(total_count).as(reactions_like),'
           'reactions.type(SAD).limit(0).summary(total_count).as(reactions_sad),'
           'reactions.type(ANGRY).limit(0).summary(total_count).as(reactions_angry),'
           'reactions.type(THANKFUL).limit(0).summary(total_count).as(reactions_thankful).summary(total_count),'
           'comments.limit(0).summary(total_count).filter(stream)&since=' +
           str(since) + '&access_token=' + access_token)

    posts = get_request_and_jsonize(url)

    if 'error' in posts:
        logging.error('%s', posts['error']['message'])
        return

    sql = "UPDATE `fb_posts` SET `compartilhamentos` = (%s), `reacoes` = (%s), `reacoes_like` = (%s), " \
          "`reacoes_haha` = (%s), `reacoes_wow` = (%s), `reacoes_sad` = (%s), `reacoes_love` = (%s), " \
          "`reacoes_angry` = (%s), `reacoes_thankful` = (%s), `comentarios` = (%s) WHERE `idfacebook` = (%s)"

    while True:
        if len(posts['data']) == 0:
            break
        for registro in posts['data']:
            idfacebook = registro['id']
            compartilhamentos = registro['shares']['count'] if 'shares' in registro else 0
            reacoes = registro['reactions']['summary']['total_count'] if 'reactions' in registro else 0
            reacoes_like = registro['reactions_like']['summary']['total_count'] if 'reactions_like' in registro else 0
            reacoes_haha = registro['reactions_haha']['summary']['total_count'] if 'reactions_haha' in registro else 0
            reacoes_wow = registro['reactions_wow']['summary']['total_count'] if 'reactions_wow' in registro else 0
            reacoes_sad = registro['reactions_sad']['summary']['total_count'] if 'reactions_sad' in registro else 0
            reacoes_love = registro['reactions_love']['summary']['total_count'] if 'reactions_love' in registro else 0
            reacoes_angry = registro['reactions_angry']['summary']['total_count'] if 'reactions_angry' in registro else 0
            reacoes_thankful = registro['reactions_thankful']['summary']['total_count'] if 'reactions_thankful' in registro else 0
            if 'comments' in registro and 'total_count' in registro['comments']['summary']:
                comentarios = registro['comments']['summary']['total_count']
            else:
                comentarios = 0

            try:
                cur.execute(sql, (compartilhamentos, reacoes, reacoes_like, reacoes_haha, reacoes_wow, reacoes_sad,
                                  reacoes_love, reacoes_angry, reacoes_thankful, comentarios, idfacebook))
            except Exception as excpt:
                logging.error('%s', excpt)
                exit()

        conn.commit()
        if 'paging' in posts:
            if 'next' in posts['paging']:
                posts = get_request_and_jsonize(posts['paging']['next'])
            else:
                break
        else:
            break


def facebook_update_comments_page(idperfil):
    since = datetime.date.today() - datetime.timedelta(days=DIAS)
    sql = 'SELECT `idpost`, `idfacebook` FROM `fb_posts` ' \
          'WHERE `idperfil` = (%s) AND comentarios > 0 AND datahora > (%s)'
    cur.execute(sql, (idperfil, since))
    posts = cur.fetchall()

    for post in posts:
        facebook_update_comments_post(post['idpost'], post['idfacebook'])


def facebook_update_comments_post(idpost, idfacepost):
    url = (graph_url + api_version + idfacepost +
           '/comments?limit=5000&order=reverse_chronological&fields=id'
           'reactions.limit(0).summary(total_count),'
           'reactions.type(LOVE).limit(0).summary(total_count).as(reactions_love),'
           'reactions.type(WOW).limit(0).summary(total_count).as(reactions_wow),'
           'reactions.type(HAHA).limit(0).summary(total_count).as(reactions_haha),'
           'reactions.type(LIKE).limit(0).summary(total_count).as(reactions_like),'
           'reactions.type(SAD).limit(0).summary(total_count).as(reactions_sad),'
           'reactions.type(ANGRY).limit(0).summary(total_count).as(reactions_angry),'
           'reactions.type(THANKFUL).limit(0).summary(total_count).as(reactions_thankful).summary(total_count),'
           'comment_count&filter=stream&access_token=' + access_token)

    comments = get_request_and_jsonize(url)

    if 'error' in comments:
        logging.error('%s', comments['error'])
        return -1

    sql = 'UPDATE `fb_comentarios` SET `curtidas` = (%s), `reacoes_like` = (%s), " \
          "`reacoes_haha` = (%s), `reacoes_wow` = (%s), `reacoes_sad` = (%s), `reacoes_love` = (%s), " \
          "`reacoes_angry` = (%s), `reacoes_thankful` = (%s), `respostas` = (%s) WHERE `idfacebook` = (%s)'

    if 'data' not in comments:
        return
    if len(comments['data']) == 0:
        return

    while True:
        for comment in comments['data']:
            idfacebook = comment['id']
            curtidas = comment['reactions']['summary']['total_count'] if 'reactions' in comment else 0
            reacoes_like = comment['reactions_like']['summary']['total_count'] if 'reactions_like' in comment else 0
            reacoes_haha = comment['reactions_haha']['summary']['total_count'] if 'reactions_haha' in comment else 0
            reacoes_wow = comment['reactions_wow']['summary']['total_count'] if 'reactions_wow' in comment else 0
            reacoes_sad = comment['reactions_sad']['summary']['total_count'] if 'reactions_sad' in comment else 0
            reacoes_love = comment['reactions_love']['summary']['total_count'] if 'reactions_love' in comment else 0
            reacoes_angry = comment['reactions_angry']['summary']['total_count'] if 'reactions_angry' in comment else 0
            reacoes_thankful = comment['reactions_thankful']['summary']['total_count'] if 'reactions_thankful' in comment else 0
            respostas = comment['comment_count']

            cur.execute(sql, (curtidas, reacoes_like, reacoes_haha, reacoes_wow, reacoes_sad, reacoes_love,
                              reacoes_angry, reacoes_thankful, respostas, idfacebook))

        conn.commit()

        if 'next' in comments['paging']:
            comments = get_request_and_jsonize(comments['paging']['next'])
        else:
            break

    logging.info('Atualizados os comentários do post %d', idpost)


def facebook_init():
    sql = 'SELECT `idempresa`, `idperfil`, `username` FROM `fb_perfis` WHERE `ultimaatualizacao` is null'
    cur.execute(sql)
    results = cur.fetchall()
    for result in results:
        idempresa = result['idempresa']
        idperfil = result['idperfil']
        username = result['username']

        idfacebook = facebook_get_page(idempresa, idperfil=idperfil, idorusername=username)
        facebook_get_page_stats(idperfil=idperfil, idfacebook=idfacebook)
        facebook_get_posts(idperfil=idperfil, idfacebook=idfacebook, datahoraultimopost=None)
        facebook_get_comments_page(idperfil, init=True)

        ultimaatualizacao = datetime.date.today()
        sql = 'UPDATE fb_perfis SET `ultimaatualizacao` = (%s) WHERE `idperfil` = (%s)'
        cur.execute(sql, (ultimaatualizacao, idperfil))
        conn.commit()


def facebook_update():
    sql = 'SELECT * FROM `fb_perfis` WHERE `ultimaatualizacao` is not null'
    cur.execute(sql)
    pages = cur.fetchall()

    for page in pages:
        idperfil = page['idperfil']
        idfacebook = page['idfacebook']
        datahoraultimopost = page['datahoraultimopost']

        ultimaatualizacao = page['ultimaatualizacao']
        datanow = datetime.date.today()
        if (datanow - ultimaatualizacao).days > DIAS:
            # TODO facebook_get_page(idperfil, idfacebook)
            ultimaatualizacao = datanow
            sql = 'UPDATE fb_perfis SET `ultimaatualizacao` = (%s) WHERE `idperfil` = (%s)'
            cur.execute(sql, (ultimaatualizacao, idperfil))
            conn.commit()

        facebook_get_page_stats(idperfil, idfacebook)
        facebook_get_posts(idperfil, idfacebook, datahoraultimopost)
        facebook_update_posts(idfacebook)
        facebook_get_comments_page(idperfil, init=False)
        facebook_update_comments_page(idperfil)


if __name__ == "__main__":
    import sys
    start_time = time.time()
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    if sys.argv[1] == 'init':
        facebook_init()
    elif sys.argv[1] == 'update':
        facebook_update()
    else:
        logging.info('Comando: python ./facebook_agente.py [init|update]')
        exit()
    logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
    logging.info("Script executado com sucesso!")

cur.close()
conn.close()
