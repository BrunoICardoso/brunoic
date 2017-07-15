import pymysql
import time
import logging
import banco
import datetime
from safe_requests import get_request_and_jsonize

api_key = 'AIzaSyAqRERW292cPCFi1nHG8qngTFvVVwuVUpE'
conn, cur = banco.conecta_banco()
MAXPOSTLENGTH = 10000
MAXTITULO = 100
DIAS = 90
pathyoutube = '/media/Shared-Images/youtube/'


def youtube_get_channel(idcanal, idyoutube, username):
    logging.info("Captura de informações do canal de id %d iniciada.", idcanal)

    if idyoutube is None:
        url = ('https://www.googleapis.com/youtube/v3/channels?part=id,snippet,contentDetails'
               '&forUsername=' + username + '&key=' + api_key)
    else:
        url = ('https://www.googleapis.com/youtube/v3/channels?part=id,snippet,contentDetails'
               '&id=' + idyoutube + '&key=' + api_key)

    search = get_request_and_jsonize(url)
    perfil = search['items'][0]
    idyoutube = perfil['id']
    nome = perfil['snippet']['title']
    descricao = perfil['snippet']['description']
    urlimagem = perfil['snippet']['thumbnails']['medium']['url']
    if '.png' in urlimagem:
        nomeimagem = idyoutube + '.png'
    else:
        nomeimagem = idyoutube + '.jpg'

    idlistavideos = perfil['contentDetails']['relatedPlaylists']['uploads']

    sql = 'UPDATE `yt_canais` SET `idyoutube` = (%s), `nome` = (%s), `descricao` = (%s), ' \
          '`urlimagem` = (%s), `nomeimagem` = (%s), `idlistavideos` = (%s) WHERE `idcanal`= (%s)'
    try:
        cur.execute(sql, (idyoutube, nome, descricao, urlimagem, nomeimagem, idlistavideos, idcanal))
        conn.commit()
    except pymysql.err.IntegrityError as excpt:
        logging.error('%s', excpt)

    return idyoutube, idlistavideos


def youtube_get_channel_stats(idcanal, idyoutube):
    hoje = datetime.date.today()
    sql = 'SELECT * FROM yt_canais_stats WHERE `idcanal` = (%s) AND DATE(datahora) = (%s)'
    cur.execute(sql, (idcanal, hoje))
    existe = cur.fetchone()
    if existe:
        logging.info('Estatísticas já capturadas hoje.')
        return

    logging.info('Iniciando captura de estatísticas do canal de id %d.', idcanal)

    url = ('https://www.googleapis.com/youtube/v3/channels?part=statistics&id=' + idyoutube + '&key=' + api_key)

    search = get_request_and_jsonize(url)

    if len(search['items']) == 0:
        logging.warning('Canal %d não existe.', idcanal)
        return

    perfil = search['items'][0]['statistics']

    qtdvisualizacoes = perfil['viewCount']
    qtdseguidores = perfil['subscriberCount']
    qtdvideos = perfil['videoCount']
    datahora = datetime.datetime.utcnow()
    sql = 'INSERT INTO `yt_canais_stats` (`idcanal`, `datahora`, `qtdvisualizacoes`, `qtdseguidores`, `qtdvideos`)' \
          'VALUES (%s, %s, %s, %s, %s)'

    cur.execute(sql, (idcanal, datahora, qtdvisualizacoes, qtdseguidores, qtdvideos))
    conn.commit()


def youtube_get_videos(idcanal, idlistavideos, datahoraultimovideo):
    logging.info('Iniciando captura de vídeos do canal de id %d.', idcanal)

    url = ('https://www.googleapis.com/youtube/v3/playlistItems?part=snippet'
           '&maxResults=50&playlistId=' + idlistavideos + '&key=' + api_key)

    search = get_request_and_jsonize(url)
    registros = search['items']

    if len(registros) == 0:
        logging.info('Nenhum vídeo no canal de id %d', idcanal)
        return

    novadatahoraultimovideo = datetime.datetime.strptime(registros[0]['snippet']['publishedAt'],
                                                         '%Y-%m-%dT%H:%M:%S.000Z')

    if datahoraultimovideo:
        if novadatahoraultimovideo <= datahoraultimovideo:
            logging.info('Nenhum vídeo novo no canal de id %d', idcanal)
            return

    sql = 'UPDATE `yt_canais` SET `datahoraultimovideo` = (%s) WHERE `idcanal` = (%s)'

    try:
        cur.execute(sql, (novadatahoraultimovideo, idcanal))
        conn.commit()
    except Exception as excpt:
        logging.critical('%s', excpt)
        exit()

    count = 0
    while True:
        for reg in registros:
            snippet = reg['snippet']
            datahora = datetime.datetime.strptime(snippet['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z')
            if datahoraultimovideo:
                if datahora <= datahoraultimovideo:
                    conn.commit()
                    logging.info('Capturados os últimos %d vídeos da página de id %d', count, idcanal)
                    return
            idyoutube = snippet['resourceId']['videoId']
            titulo = snippet['title'] if len(snippet['title']) <= MAXTITULO else snippet['title'][:MAXTITULO]
            descricao = snippet['description']
            urlimagem = snippet['thumbnails']['standard']['url'] if 'standard' in snippet['thumbnails'] \
                else snippet['thumbnails']['high']['url']
            if '.png' in urlimagem:
                nomeimagem = idyoutube + '.png'
            else:
                nomeimagem = idyoutube + '.jpg'

            # safe_retrieve(urlimagem, pathyoutube + nomeimagem)

            url = ('https://www.googleapis.com/youtube/v3/videos?part=statistics&id=' + idyoutube + '&key=' + api_key)
            details = get_request_and_jsonize(url)
            statistics = details['items'][0]['statistics'] if 'statistics' in details['items'][0] else None
            if statistics:
                qtdvisualizacoes = statistics['viewCount'] if 'viewCount' in statistics else 0
                qtdcurtidas = statistics['likeCount'] if 'likeCount' in statistics else 0
                qtddescurtidas = statistics['dislikeCount'] if 'dislikeCount' in statistics else 0
                qtdfavoritados = statistics['favoriteCount'] if 'favoriteCount' in statistics else 0
                qtdcomentarios = statistics['commentCount'] if 'commentCount' in statistics else 0
            else:
                qtdvisualizacoes = qtdcurtidas = qtddescurtidas = qtdfavoritados = qtdcomentarios = 0

            sql = 'INSERT INTO `yt_videos` (`idcanal`, `idyoutube`, `titulo`, `descricao`, `datahora`, ' \
                  '`qtdvisualizacoes`, `qtdcurtidas`, `qtddescurtidas`, `qtdfavoritados`, `qtdcomentarios`, ' \
                  '`urlimagem`, `nomeimagem`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
            try:
                cur.execute(sql, (idcanal, idyoutube, titulo, descricao, datahora, qtdvisualizacoes,
                                  qtdcurtidas, qtddescurtidas, qtdfavoritados, qtdcomentarios, urlimagem,
                                  nomeimagem))
                count += 1
            except pymysql.err.IntegrityError as excpt:
                logging.error('%s', excpt)

        conn.commit()
        logging.info('Últimos %d vídeos do canal de id %d capturados.', count, idcanal)

        if 'nextPageToken' in search:
            url = ('https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&pageToken='
                   + search['nextPageToken'] + '&playlistId=' + idlistavideos + '&key=' + api_key)
            search = get_request_and_jsonize(url)
            registros = search['items']
        else:
            break


def youtube_getupdate_comments_channel(idcanal, init):
    logging.info('Iniciando captura/atualização de comentários do canal %d.', idcanal)

    since = datetime.date.today() - datetime.timedelta(days=DIAS)

    if init is True:
        sql = 'SELECT `idvideo`, `idyoutube`, `datahoraultimocomentario` FROM `yt_videos` ' \
              'WHERE `idcanal` = (%s) AND qtdcomentarios > 0'
        cur.execute(sql, idcanal)
    else:
        sql = 'SELECT `idvideo`, `idyoutube`, `datahoraultimocomentario` FROM `yt_videos`' \
              'WHERE `idcanal` = (%s) AND `datahora` >= (%s) AND qtdcomentarios > 0'
        cur.execute(sql, (idcanal, since))

    results = cur.fetchall()
    for video in results:
        youtube_get_first_order_comments_video(video['idvideo'], video['idyoutube'], video['datahoraultimocomentario'])
        if not init:
            youtube_update_first_order_comments_video(video['idvideo'], video['idyoutube'])
        youtube_getupdate_replies_video(video['idvideo'], get=True)
        if not init:
            youtube_getupdate_replies_video(video['idvideo'], get=False)


def youtube_get_first_order_comments_video(idvideo, idvideo_youtube, datahoraultimocomentario):
    url = ('https://www.googleapis.com/youtube/v3/commentThreads?part=id,snippet'
           '&maxResults=100&textFormat=plainText&videoId=' + idvideo_youtube + '&key=' + api_key)

    search = get_request_and_jsonize(url)
    if 'items' in search:
        registros = search['items']
    else:
        logging.info('Comentários desativados para o vídeo de id %d', idvideo)
        return

    if len(registros) > 0:
        novadatahoraultimocomentario = datetime.datetime.strptime(registros[0]['snippet']['topLevelComment']
                                                                  ['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z')
    else:
        logging.info('Nenhum comentário no vídeo de id %d', idvideo)
        return

    if datahoraultimocomentario:
        if novadatahoraultimocomentario <= datahoraultimocomentario:
            logging.info('Nenhum comentário novo no vídeo de id %d', idvideo)
            return

    sql = 'UPDATE `yt_videos` SET `datahoraultimocomentario` = (%s) WHERE idvideo = (%s)'
    cur.execute(sql, (novadatahoraultimocomentario, idvideo))
    conn.commit()

    sql = 'INSERT INTO yt_comentarios (`idyoutube`, `idvideo`, `postagem`, `qtdcurtidas`, `datahora`, `qtdrespostas`,' \
          '`nomeusuario`, `idusuario`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

    # Loop de captura de comentários de um dado vídeo
    count = 0
    while True:
        for reg in registros:
            idyoutube = reg['id']
            comentario = reg['snippet']['topLevelComment']['snippet']
            postagem = comentario['textDisplay'] if len(comentario['textDisplay']) <= MAXPOSTLENGTH \
                else comentario['textDisplay'][:MAXPOSTLENGTH]
            qtdcurtidas = comentario['likeCount']
            datahora = datetime.datetime.strptime(comentario['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z')
            if datahoraultimocomentario:
                if datahora <= datahoraultimocomentario:
                    conn.commit()
                    logging.info('Capturados os últimos %d comentários do vídeo %d', count, idvideo)
                    return
            qtdrespostas = reg['snippet']['totalReplyCount']
            nomeusuario = comentario['authorDisplayName']
            idusuario = comentario['authorChannelId']['value'] if 'authorChannelId' in comentario else None

            try:
                cur.execute(sql, (idyoutube, idvideo, postagem, qtdcurtidas,
                                  datahora, qtdrespostas, nomeusuario, idusuario))
                count += 1
            except pymysql.err.IntegrityError as excpt:
                logging.error('%s', excpt)

        conn.commit()
        logging.info('Capturados os últimos %d comentários do vídeo de id %d', count, idvideo)

        if 'nextPageToken' in search:
            next_page_token = search['nextPageToken']
            url = ('https://www.googleapis.com/youtube/v3/commentThreads?part=id,replies,snippet&maxResults=100'
                   '&textFormat=plainText&videoId=' + idvideo_youtube + '&pageToken=' +
                   next_page_token + '&key=' + api_key)
            search = get_request_and_jsonize(url)
            if 'items' in search:
                registros = search['items']
            else:
                return
        else:
            break


def youtube_getupdate_replies_video(idvideo, get):
    sql = 'SELECT `idcomentario`, `idyoutube`, `datahoraultimoreply` FROM yt_comentarios ' \
          'WHERE qtdrespostas > 0 AND `idvideo` = (%s)'
    cur.execute(sql, idvideo)

    results = cur.fetchall()
    if len(results) == 0:
        return

    count = 0
    if get is True:
        for comment in results:
            rep = youtube_get_replies_comment(comment['idcomentario'], comment['idyoutube'],
                                              comment['datahoraultimoreply'], idvideo)
            if rep:
                count += rep
        logging.info('Capturados os últimos %d replies do vídeo %d', count, idvideo)
    else:
        for comment in results:
            rep = youtube_update_replies_comment(comment['idyoutube'])
            if rep:
                count += rep
        logging.info('Atualizados os últimos %d replies do vídeo %d', count, idvideo)


def youtube_get_replies_comment(idcomentarioresposta, idyoutube_comentario, datahoraultimoreply, idvideo):
    url = ('https://www.googleapis.com/youtube/v3/comments?part=id,snippet'
           '&maxResults=100&textFormat=plainText&parentId=' + idyoutube_comentario
           + '&key=' + api_key)

    search = get_request_and_jsonize(url)

    if 'items' not in search:
        return

    registros = search['items']

    if len(registros) == 0:
        return

    novadatahoraultimoreply = datetime.datetime.strptime(registros[0]['snippet']['publishedAt'],
                                                         '%Y-%m-%dT%H:%M:%S.000Z')
    if datahoraultimoreply:
        if novadatahoraultimoreply <= datahoraultimoreply:
            logging.info('Nenhum reply novo no comentário de id %d', idcomentarioresposta)
            return

    sql = 'UPDATE `yt_comentarios` SET `datahoraultimoreply` = (%s) WHERE idcomentario = (%s)'
    cur.execute(sql, (novadatahoraultimoreply, idcomentarioresposta))
    conn.commit()

    sql = 'INSERT INTO yt_comentarios (`idyoutube`, `idvideo`, `idcomentarioresposta`, `postagem`, `qtdcurtidas`, ' \
          '`datahora`, `qtdrespostas`, `nomeusuario`, `idusuario`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'

    count = 0
    while True:
        for reply in registros:
            idyoutube = reply['id']
            snippet = reply['snippet']
            datahora = datetime.datetime.strptime(snippet['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z')
            if datahoraultimoreply:
                if datahora <= datahoraultimoreply:
                    conn.commit()
                    return count
            # idvideo já setado
            postagem = snippet['textDisplay'] if len(snippet['textDisplay']) <= MAXPOSTLENGTH \
                else snippet['textDisplay'][:MAXPOSTLENGTH]
            qtdcurtidas = snippet['likeCount']
            qtdrespostas = None
            nomeusuario = snippet['authorDisplayName']
            idusuario = snippet['authorChannelId']['value']
            try:
                cur.execute(sql, (idyoutube, idvideo, idcomentarioresposta, postagem, qtdcurtidas,
                                  datahora, qtdrespostas, nomeusuario, idusuario))
                count += 1
            except pymysql.err.IntegrityError as excpt:
                logging.error('%s', excpt)

        conn.commit()

        if 'nextPageToken' in search:
            next_page_token = search['nextPageToken']
            url = ('https://www.googleapis.com/youtube/v3/comments?part=id,snippet'
                   '&maxResults=100&textFormat=plainText&parentId=' + idyoutube_comentario + '&pageToken=' +
                   next_page_token + '&key=' + api_key)
            search = get_request_and_jsonize(url)
            registros = search['items']
        else:
            return count


def youtube_update_channel(idcanal, idyoutube):
    logging.info("Atualização de informações do canal de id %d iniciada.", idcanal)

    url = ('https://www.googleapis.com/youtube/v3/channels?part=id,snippet,contentDetails'
           '&id=' + idyoutube + '&key=' + api_key)

    search = get_request_and_jsonize(url)
    if len(search['items']) == 0:
        logging.error('Perfil possivelmente não existe mais.')
        return -1

    perfil = search['items'][0]
    nome = perfil['snippet']['title']
    descricao = perfil['snippet']['description']
    urlimagem = perfil['snippet']['thumbnails']['medium']['url']
    ultimaatualizacao = datetime.date.today()

    sql = 'UPDATE `yt_canais` SET `nome` = (%s), `descricao` = (%s), `urlimagem` = (%s),' \
          '`ultimaatualizacao` = (%s) WHERE `idcanal` = (%s)'
    try:
        cur.execute(sql, (nome, descricao, urlimagem, ultimaatualizacao, idcanal))
        conn.commit()
    except Exception as excpt:
        logging.error('%s', excpt)


def youtube_update_videos(idcanal, idlistavideos):
    logging.info('Iniciando função de atualização de vídeos do canal %d com até 90 dias.', idcanal)

    url = ('https://www.googleapis.com/youtube/v3/playlistItems?part=snippet'
           '&maxResults=50&playlistId=' + idlistavideos + '&key=' + api_key)

    search = get_request_and_jsonize(url)
    registros = search['items']

    if len(registros) == 0:
        logging.info('Nenhum vídeo no canal de id %d', idcanal)
        return

    since = datetime.date.today() - datetime.timedelta(days=DIAS)

    sql = 'UPDATE yt_videos SET `qtdvisualizacoes` = (%s), `qtdcurtidas` = (%s), `qtddescurtidas` = (%s), ' \
          '`qtdfavoritados` = (%s), `qtdcomentarios` = (%s) WHERE `idyoutube` = (%s)'

    count = 0
    while True:
        for reg in registros:
            snippet = reg['snippet']
            idyoutube = snippet['resourceId']['videoId']
            datahora = datetime.datetime.strptime(snippet['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z')
            if datahora.date() < since:
                conn.commit()
                logging.info('Atualizados os últimos %d vídeos', count)
                return
            url = ('https://www.googleapis.com/youtube/v3/videos?part=statistics&id=' + idyoutube + '&key=' + api_key)
            details = get_request_and_jsonize(url)
            statistics = details['items'][0]['statistics'] if 'statistics' in details['items'][0] else None
            if statistics:
                qtdvisualizacoes = statistics['viewCount'] if 'viewCount' in statistics else 0
                qtdcurtidas = statistics['likeCount'] if 'likeCount' in statistics else 0
                qtddescurtidas = statistics['dislikeCount'] if 'dislikeCount' in statistics else 0
                qtdfavoritados = statistics['favoriteCount'] if 'favoriteCount' in statistics else 0
                qtdcomentarios = statistics['commentCount'] if 'commentCount' in statistics else 0
            else:
                qtdvisualizacoes = qtdcurtidas = qtddescurtidas = qtdfavoritados = qtdcomentarios = 0
            try:
                cur.execute(sql, (qtdvisualizacoes, qtdcurtidas, qtddescurtidas,
                                  qtdfavoritados, qtdcomentarios, idyoutube))
                count += 1
            except pymysql.err.IntegrityError as excpt:
                logging.error('%s', excpt)

        conn.commit()
        logging.info('Últimos %d vídeos do canal de id %d atualizados.', count, idcanal)

        if 'nextPageToken' in search:
            url = ('https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&pageToken='
                   + search['nextPageToken'] + '&playlistId=' + idlistavideos + '&key=' + api_key)
            search = get_request_and_jsonize(url)
            registros = search['items']
        else:
            break


def youtube_update_first_order_comments_video(idvideo, idvideo_youtube):
    url = ('https://www.googleapis.com/youtube/v3/commentThreads?part=id,snippet'
           '&maxResults=100&textFormat=plainText&videoId=' + idvideo_youtube + '&key=' + api_key)

    search = get_request_and_jsonize(url)
    if 'items' in search:
        registros = search['items']
    else:
        logging.info('Comentários desativados para o vídeo de id %d', idvideo)
        return

    if len(registros) == 0:
        logging.info('Nenhum comentário no vídeo de id %d', idvideo)
        return

    sql = 'UPDATE yt_comentarios SET `qtdcurtidas` = (%s), `qtdrespostas` = (%s) WHERE `idyoutube` = (%s)'

    # Loop de captura de comentários de um dado vídeo
    count = 0
    while True:
        for reg in registros:
            idyoutube = reg['id']
            comentario = reg['snippet']['topLevelComment']['snippet']
            qtdcurtidas = comentario['likeCount']
            qtdrespostas = reg['snippet']['totalReplyCount']
            try:
                cur.execute(sql, (qtdcurtidas, qtdrespostas, idyoutube))
                count += 1
            except pymysql.err.IntegrityError as excpt:
                logging.error('%s', excpt)

        conn.commit()
        logging.info('Atualizados os últimos %d comentários do vídeo de id %d', count, idvideo)

        if 'nextPageToken' in search:
            next_page_token = search['nextPageToken']
            url = ('https://www.googleapis.com/youtube/v3/commentThreads?part=id,replies,snippet&maxResults=100'
                   '&textFormat=plainText&videoId=' + idvideo_youtube + '&pageToken=' +
                   next_page_token + '&key=' + api_key)
            search = get_request_and_jsonize(url)
            if 'items' in registros:
                registros = search['items']
            else:
                break
        else:
            break


def youtube_update_replies_comment(idyoutube_comentario):
    url = ('https://www.googleapis.com/youtube/v3/comments?part=id,snippet'
           '&maxResults=100&textFormat=plainText&parentId=' + idyoutube_comentario
           + '&key=' + api_key)

    search = get_request_and_jsonize(url)
    if 'items' not in search:
        logging.info('Nenhum reply encontrado.')
        return

    registros = search['items']

    sql = 'UPDATE yt_comentarios SET `qtdcurtidas` = (%s) WHERE `idyoutube` = (%s)'

    count = 0
    while True:
        for reply in registros:
            idyoutube = reply['id']
            snippet = reply['snippet']
            qtdcurtidas = snippet['likeCount']
            try:
                cur.execute(sql, (qtdcurtidas, idyoutube))
                count += 1
            except pymysql.err.IntegrityError as excpt:
                logging.error('%s', excpt)

        conn.commit()

        if 'nextPageToken' in search:
            next_page_token = search['nextPageToken']
            url = ('https://www.googleapis.com/youtube/v3/comments?part=id,snippet'
                   '&maxResults=100&textFormat=plainText&parentId=' + idyoutube_comentario + '&pageToken=' +
                   next_page_token + '&key=' + api_key)
            search = get_request_and_jsonize(url)
            registros = search['items']
        else:
            return count


def youtube_init():
    sql = 'SELECT `idcanal`, `idyoutube`, `username` FROM `yt_canais` WHERE `ultimaatualizacao` is null'
    cur.execute(sql)
    results = cur.fetchall()

    if len(results) == 0:
        logging.info('Nenhum canal novo a ser capturado.')
        exit()

    for result in results:
        username = result['username']
        idyoutube = result['idyoutube']
        idcanal = result['idcanal']
        idyoutube, idlistavideos = youtube_get_channel(idcanal, idyoutube, username)
        youtube_get_channel_stats(idcanal, idyoutube)
        youtube_get_videos(idcanal, idlistavideos, None)
        youtube_getupdate_comments_channel(idcanal, init=True)

        ultimaatualizacao = datetime.date.today()
        sql = 'UPDATE yt_canais SET `ultimaatualizacao` = (%s) WHERE `idcanal` = (%s)'
        cur.execute(sql, (ultimaatualizacao, idcanal))
        conn.commit()


def youtube_update():
    sql = 'SELECT * from `yt_canais` WHERE ultimaatualizacao is not null'
    cur.execute(sql)
    results = cur.fetchall()

    for result in results:
        idcanal = result['idcanal']
        idyoutube = result['idyoutube']
        idlistavideos = result['idlistavideos']
        datahoraultimovideo = result['datahoraultimovideo']
        ultimaatualizacao = result['ultimaatualizacao']
        datanow = datetime.date.today()

        if (datanow - ultimaatualizacao).days > DIAS:
            canal_valido = youtube_update_channel(idcanal, idyoutube)
            if canal_valido == -1:
                continue
            ultimaatualizacao = datanow
            sql = 'UPDATE yt_canais SET `ultimaatualizacao` = (%s) WHERE `idcanal` = (%s)'
            cur.execute(sql, (ultimaatualizacao, idcanal))
            conn.commit()

        youtube_get_channel_stats(idcanal, idyoutube)
        youtube_get_videos(idcanal, idlistavideos, datahoraultimovideo)
        youtube_update_videos(idcanal, idlistavideos)
        youtube_getupdate_comments_channel(idcanal, init=False)


if __name__ == "__main__":
    import sys
    start_time = time.time()
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    if sys.argv[1] == 'init':
        youtube_init()
    elif sys.argv[1] == 'update':
        youtube_update()
    else:
        logging.info('Comando: python3 youtube_agente.py [init|update]')
        exit()
    logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
    logging.info("Script executado com sucesso!")

cur.close()
conn.close()
