import logging
import time
import datetime

import banco
import twitter
from safe_requests import safe_retrieve


def conecta_api_twitter():
    apihandler = twitter.Api(consumer_key='aU9SRzmp5vf391jGfoLaNaNub',
                             consumer_secret='e18jqd8AX1PnJmVS5SbiBvlIgSbn0MnNTYhtCqJHjl741Hbfuc',
                             access_token_key='775549446688038912-j3FhaGepMmI0cMgMfoRXOsbTt7ijdae',
                             access_token_secret='rXTqYD8FM7pEAEFv4eK6uyh4Cnkq7q4obL2trjl6FQh2G')
    return apihandler


api = conecta_api_twitter()
conn, cur = banco.conecta_banco()
DIAS = 90
pathtwitter = '/media/Shared-Images/twitter/'


def twitter_get_perfil(idperfil, screenname):
    logging.info('Iniciando captura do perfil %s', screenname)

    user = api.GetUser(screen_name=screenname)

    idtwitter = str(user.id)
    nome = user.name
    descricao = user.description
    urlimagem = user.profile_image_url
    if '.png' in urlimagem:
        nomeimagem = idtwitter + '.png'
    else:
        nomeimagem = idtwitter + '.jpg'

    # TODO safe_retrieve(urlimagem, pathtwitter + nomeimagem)

    sql = "UPDATE `tw_perfis` SET `idtwitter` = (%s), `nome` = (%s), `descricao` = (%s), `urlimagem` = (%s)," \
          " `nomeimagem` = (%s) WHERE `idperfil`= (%s)"
    try:
        cur.execute(sql, (idtwitter, nome, descricao, urlimagem, nomeimagem, idperfil))
        conn.commit()
    except Exception as excpt:
        logging.error('%s', excpt)

    return idtwitter


def twitter_get_perfil_stats(idperfil, idtwitter):
    logging.info('Iniciando captura de estatísticas do perfil de id %d', idperfil)

    hoje = datetime.date.today()
    sql = 'SELECT * FROM tw_perfis_stats WHERE `idperfil` = (%s) AND DATE(datahora) = (%s)'
    cur.execute(sql, (idperfil, hoje))
    existe = cur.fetchone()
    if existe:
        logging.info('Estatísticas já capturadas hoje.')
        return

    user = api.GetUser(user_id=idtwitter)
    qtdseguidores = user.followers_count
    qtdseguidos = user.friends_count
    qtdfavoritado = user.favourites_count
    qtdlistado = user.listed_count
    qtdtweets = user.statuses_count
    datahora = datetime.datetime.utcnow()

    sql = "INSERT INTO `tw_perfis_stats` (`idperfil`, `datahora`, `qtdseguidores`, `qtdseguidos`, `qtdfavoritado`," \
          "`qtdlistado`, `qtdtweets`) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    cur.execute(sql, (idperfil, datahora, qtdseguidores, qtdseguidos, qtdfavoritado, qtdlistado, qtdtweets))
    conn.commit()


def twitter_get_posts(idperfil, userid, ultimotwcapturado):
    if ultimotwcapturado is None:
        status = api.GetUserTimeline(user_id=userid, include_rts=1, count=200)
    else:
        status = api.GetUserTimeline(user_id=userid, include_rts=1, count=200, since_id=ultimotwcapturado)

    if len(status) == 0:
        logging.info('Nenhum novo tweet para o perfil de id %d', idperfil)
        return

    novoultimotwcapturado = status[0].id_str
    sql = "UPDATE `tw_perfis` SET `ultimotwcapturado` = (%s) where `idperfil`=(%s)"
    cur.execute(sql, (novoultimotwcapturado, idperfil))
    conn.commit()

    sql = "INSERT INTO `tw_posts` (`idperfil`, `idtwitter`, `postagem`, `datahora`, `is_retweet`," \
          " `is_reply`, `qtdretweets`, `qtdfavoritado`, `idtwrespondido`, `urlimagem`, `nomeimagem` )" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    count = 0
    while True:
        for registro in status:
            idtwitter = registro.id_str
            postagem = registro.text
            datahora = datetime.datetime.strptime(registro.created_at, '%a %b %d %H:%M:%S +0000 %Y')
            is_retweet = True if registro.retweeted_status else False
            if registro.in_reply_to_status_id:
                is_reply = True
                idtwrespondido = registro.in_reply_to_status_id
            else:
                is_reply = False
                idtwrespondido = None
            qtdretweets = registro.retweet_count
            qtdfavoritado = registro.favorite_count

            urlimagem = None
            nomeimagem = None
            if registro.media:
                urlimagem = registro.media[0].media_url
                if '.png' in urlimagem:
                    nomeimagem = idtwitter + '.png'
                else:
                    nomeimagem = idtwitter + '.jpg'

                # safe_retrieve(urlimagem, pathtwitter + nomeimagem)

            try:
                cur.execute(sql, (idperfil, idtwitter, postagem, datahora, is_retweet, is_reply,
                                  qtdretweets, qtdfavoritado, idtwrespondido, urlimagem, nomeimagem))
                count += 1
            except Exception as excpt:
                logging.error('%s', excpt)

        conn.commit()

        logging.info('Capturados os últimos %d tweets da página', count)

        max_id = int(idtwitter) - 1  # para não incluir o último tweet na busca
        if ultimotwcapturado is None:
            status = api.GetUserTimeline(user_id=userid, include_rts=1, count=200, max_id=str(max_id))
        else:
            status = api.GetUserTimeline(user_id=userid, include_rts=1, count=200,
                                         max_id=str(max_id), since_id=ultimotwcapturado)
        if len(status) == 0:
            break


def twitter_get_mentions(idperfil, screenname, ultimotwmencaocapturado):
    if ultimotwmencaocapturado is None:
        logging.info('Iniciada a captura de menções ao perfil de id %d', idperfil)
        status = api.GetSearch(raw_query=("q=%40" + screenname + "&result_type=recent&count=100"))
    else:
        logging.info('Iniciada a captura de novas menções ao perfil de id %d', idperfil)
        status = api.GetSearch(raw_query=("q=%40" + screenname + "&result_type=recent&count=100&since_id="
                                          + str(ultimotwmencaocapturado)))

    if len(status) == 0:
        logging.info('Nenhuma nova menção à página de id %d', idperfil)
        return

    novoultimotwmencaocapturado = status[0].id_str
    sql = "UPDATE `tw_perfis` SET `ultimotwmencaocapturado` = (%s) where `idperfil`=(%s)"
    cur.execute(sql, (novoultimotwmencaocapturado, idperfil))
    conn.commit()

    sql = "INSERT INTO `tw_mencoes` (`idperfil`, `idtwitter`, `idtwrespondido`, `postagem`, `datahora`," \
          " `is_retweet`, `is_reply`, `qtdretweets`, `qtdfavoritado`, `urlimagem`, `nomeimagem`, " \
          "`nomeusuario`, `idusuario` ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    count = 0
    while True:
        if len(status) == 0:
            break
        for registro in status:
            idtwitter = registro.id_str
            postagem = registro.text
            datahora = datetime.datetime.strptime(registro.created_at, '%a %b %d %H:%M:%S +0000 %Y')
            if registro.retweeted_status:
                is_retweet = True
            else:
                is_retweet = False
            if registro.in_reply_to_status_id:
                idtwrespondido = registro.in_reply_to_status_id
                is_reply = True
            else:
                idtwrespondido = None
                is_reply = False
            qtdretweets = registro.retweet_count
            qtdfavoritado = registro.favorite_count

            urlimagem = None
            nomeimagem = None
            if registro.media:
                urlimagem = registro.media[0].media_url
                if '.png' in urlimagem:
                    nomeimagem = idtwitter + '.png'
                else:
                    nomeimagem = idtwitter + '.jpg'

                # safe_retrieve(urlimagem, nomeimagem)

            nomeusuario = registro.user.name
            idusuario = registro.user.id
            try:
                cur.execute(sql, (idperfil, idtwitter, idtwrespondido, postagem, datahora,
                                  is_retweet, is_reply, qtdretweets, qtdfavoritado, urlimagem,
                                  nomeimagem, nomeusuario, idusuario))
                count += 1
            except Exception as excpt:
                logging.error('%s', excpt)

        conn.commit()

        logging.info('Capturadas as últimas %d menções feitas ao perfil de id %d', count, idperfil)

        max_id = int(idtwitter) - 1  # para não incluir o último tweet na busca

        status = api.GetSearch(
            raw_query=("q=%40" + screenname + "&result_type=recent&count=100&max_id=" + str(max_id) +
                       "&since_id=" + str(ultimotwmencaocapturado)))


def twitter_update_posts(idperfil, userid):
    since = datetime.date.today() - datetime.timedelta(days=DIAS)

    sql = 'SELECT `idtwitter` FROM `tw_posts` WHERE `idperfil` = (%s) AND `datahora` > (%s) ORDER BY `datahora` LIMIT 1'
    cur.execute(sql, (idperfil, since))
    result = cur.fetchone()
    if result:
        since_id = result['idtwitter']
    else:
        return

    status = api.GetUserTimeline(user_id=userid, include_rts=1, count=200, since_id=since_id)

    count = 0
    while True:
        if len(status) == 0:
            break

        for registro in status:
            idtwitter = registro.id_str
            qtdretweets = registro.retweet_count
            qtdfavoritado = registro.favorite_count
            sql = "UPDATE `tw_posts` SET `qtdretweets` = (%s), `qtdfavoritado` = (%s) WHERE `idtwitter` = (%s)"

            cur.execute(sql, (qtdretweets, qtdfavoritado, idtwitter))

        conn.commit()

        count += len(status)
        logging.info('Atualizados os últimos %d tweets da página de id %d', count, idperfil)

        max_id = int(idtwitter) - 1  # para não incluir o último tweet na busca

        status = api.GetUserTimeline(user_id=userid, include_rts=1, count=200, max_id=max_id, since_id=since_id)


def twitter_update_mentions(screenname):
    try:
        status = api.GetSearch(raw_query=("q=%40" + screenname + "&result_type=recent&count=100"))
    except twitter.error.TwitterError as excpt:
        logging.critical('%s', excpt)
        exit()

    if len(status) == 0:
        logging.info('Nenhuma menção à página %s', screenname)
        return

    count = 0
    while True:
        for registro in status:
            idtwitter = registro.id_str
            qtdretweets = registro.retweet_count
            qtdfavoritado = registro.favorite_count
            sql = "UPDATE `tw_mencoes` SET `qtdretweets` = (%s), `qtdfavoritado` = (%s) WHERE `idtwitter` = (%s)"

            cur.execute(sql, (qtdretweets, qtdfavoritado, idtwitter))

        conn.commit()

        count += len(status)
        logging.info('Atualizadas as últimas %d menções à página', count)

        max_id = int(idtwitter) - 1  # para não incluir o último tweet na busca

        try:
            status = api.GetSearch(
                raw_query=("q=%40" + screenname + "&result_type=recent&count=100&max_id=" + str(max_id)))
        except twitter.error.TwitterError as excpt:
            logging.error('%s', excpt)
            exit()

        if len(status) == 0:
            break


def twitter_update():
    sql = 'SELECT * FROM `tw_perfis` WHERE `ultimaatualizacao` is not null'
    cur.execute(sql)
    perfis = cur.fetchall()

    for perfil in perfis:
        idperfil = perfil['idperfil']
        idtwitter = perfil['idtwitter']
        ultimotwcapturado = perfil['ultimotwcapturado']
        ultimotwmencaocapturado = perfil['ultimotwmencaocapturado']
        screenname = perfil['screenname']

        ultimaatualizacao = perfil['ultimaatualizacao']
        datanow = datetime.date.today()

        if (datanow - ultimaatualizacao).days > 90:
            twitter_get_perfil(idperfil, screenname)
            ultimaatualizacao = datanow
            sql = 'UPDATE tw_perfis SET `ultimaatualizacao` = (%s) WHERE `idperfil` = (%s)'
            cur.execute(sql, (ultimaatualizacao, idperfil))
            conn.commit()

        twitter_get_perfil_stats(idperfil, idtwitter)
        twitter_get_posts(idperfil, idtwitter, ultimotwcapturado)
        # twitter_get_mentions(idperfil, screenname, ultimotwmencaocapturado)
        twitter_update_posts(idperfil, idtwitter)
        # twitter_update_mentions(screenname)


def twitter_init():
    sql = 'SELECT `idperfil`, `screenname` FROM `tw_perfis` WHERE `ultimaatualizacao` is null'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        idperfil = result['idperfil']
        screenname = result['screenname']

        idtwitter = twitter_get_perfil(idperfil, screenname)
        twitter_get_perfil_stats(idperfil, idtwitter)
        twitter_get_posts(idperfil, idtwitter, ultimotwcapturado=None)
        twitter_get_mentions(idperfil, screenname, ultimotwmencaocapturado=None)

        ultimaatualizacao = datetime.date.today()
        sql = 'UPDATE tw_perfis SET `ultimaatualizacao` = (%s) WHERE `idperfil` = (%s)'
        cur.execute(sql, (ultimaatualizacao, idperfil))
        conn.commit()


if __name__ == "__main__":
    import sys
    start_time = time.time()
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    if sys.argv[1] == 'init':
        twitter_init()
    elif sys.argv[1] == 'update':
        twitter_update()
    else:
        logging.info('Comando: python ./twitter_agente.py [init|update]')
        exit()
    logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
    logging.info("Script executado com sucesso!")

cur.close()
conn.close()
