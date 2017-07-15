import logging
import datetime

import banco
import time
from elasticsearch import Elasticsearch

es = Elasticsearch(['172.16.0.85:9200'])
conn, cur = banco.conecta_banco()
indice = "dev"

query = {
    "query": {
        "match_all": {

        }
    },
    "size": 1,
    "sort": [
        {
            "idpost": {
                "order": "desc"
            }
        }
    ]
}

start_time = time.time()


def fb_importador():
    res = es.search(index=indice + "fb_posts", doc_type='fb_post', body=query)

    if len(res['hits']['hits']) > 0:
        ultimo_post = res['hits']['hits'][0]["_source"]["idpost"]
    else:
        ultimo_post = 0

    sql = 'SELECT ' \
          'po.idpost, po.idperfil, pe.idempresa, po.nomeimagem, po.postagem, po.comentarios, po.reacoes, po.compartilhamentos, po.datahora ' \
          'FROM fb_posts po INNER JOIN fb_perfis pe ON po.idperfil = pe.idperfil ' \
          'WHERE po.idpost > (%s)'

    cur.execute(sql, ultimo_post)
    results = cur.fetchall()

    importa_face(results)

    since_date = datetime.date.today() - datetime.timedelta(days=90)
    sql_update = 'SELECT ' \
                 'po.idpost, po.idperfil, pe.idempresa, po.nomeimagem, po.postagem, po.comentarios, po.reacoes, ' \
                 'po.compartilhamentos, po.datahora ' \
                 'FROM fb_posts po INNER JOIN fb_perfis pe ON po.idperfil = pe.idperfil ' \
                 'WHERE po.idpost <= (%s) AND datahora >= (%s)'

    cur.execute(sql_update, (ultimo_post, since_date))
    results = cur.fetchall()

    importa_face(results)


def importa_face(results):
    for post in results:
        postelast = {
            'idpost': post['idpost'],
            'idperfil': post['idperfil'],
            'idempresa': post['idempresa'],
            'postagem': post['postagem'],
            'compartilhamentos': post['compartilhamentos'],
            'reacoes': post['reacoes'],
            'qtdcomentarios': post['comentarios'],
            'nomeimagem': post['nomeimagem'],
            'datahora': post['datahora'].strftime("%d/%m/%y %H:%M:%S")
        }

        sql_nomeempresa = 'SELECT nome FROM empresas WHERE idempresa = (%s)'
        cur.execute(sql_nomeempresa, post['idempresa'])
        nomeempresa = cur.fetchone()['nome']
        postelast['nomeempresa'] = nomeempresa

        sqlpromo = 'SELECT promo.idpromocao, promo.nome ' \
                   'FROM promo_fb_posts pfp INNER JOIN promo_promocoes promo ON pfp.idpromocao = promo.idpromocao ' \
                   'WHERE idpost = (%s)'

        cur.execute(sqlpromo, post['idpost'])
        promos = cur.fetchall()
        promos_json = []

        for promo in promos:
            promos_json.append({'idpromocao': promo["idpromocao"], 'nome': promo["nome"]})

        postelast['promocoes'] = promos_json

        sql_post_comentarios = 'SELECT * FROM fb_comentarios WHERE idpost = (%s)'
        cur.execute(sql_post_comentarios, post['idpost'])
        comentarios = cur.fetchall()
        comentarios_json = []

        comentarios_primeira_ordem = [com for com in comentarios if com['idcomentarioresposta'] is None]
        for comentario in comentarios_primeira_ordem:
            # noinspection PyTypeChecker
            comentario_json = {
                'postagem': comentario['postagem'],
                'curtidas': comentario['curtidas'],
                'datahora': comentario['datahora'].strftime("%d/%m/%y %H:%M:%S"),
                'nomeusuario': comentario['nomeusuario'],
                'urlimagem': comentario['urlimagem']
            }
            respostas_json = []
            # noinspection PyTypeChecker
            respostas_comentario = [resp for resp in comentarios if
                                    resp['idcomentarioresposta'] == comentario['idfacebook']]
            for resposta in respostas_comentario:
                # noinspection PyTypeChecker
                resposta_json = {
                    'postagem': resposta['postagem'],
                    'curtidas': resposta['curtidas'],
                    'datahora': resposta['datahora'].strftime("%d/%m/%y %H:%M:%S"),
                    'nomeusuario': resposta['nomeusuario'],
                    'urlimagem': resposta['urlimagem']
                }
                respostas_json.append(resposta_json)
            comentario_json['respostas'] = respostas_json
            comentarios_json.append(comentario_json)

        postelast['comentarios'] = comentarios_json

        es.index(index=indice + "fb_posts", doc_type='fb_post', body=postelast, id=postelast["idpost"])


def tw_importador():
    res = es.search(index=indice + "tw_posts", doc_type='tw_post', body=query)

    if len(res['hits']['hits']) > 0:
        ultimo_post = res['hits']['hits'][0]["_source"]["idpost"]
    else:
        ultimo_post = 0

    sql = 'SELECT ' \
          'po.idpost, po.idperfil, pe.idempresa, po.nomeimagem, po.qtdretweets, po.postagem, po.qtdfavoritado, po.datahora ' \
          'FROM tw_posts po INNER JOIN tw_perfis pe ON po.idperfil = pe.idperfil ' \
          'WHERE po.idpost > (%s)'

    cur.execute(sql, ultimo_post)
    results = cur.fetchall()

    importa_twitter(results)

    since_date = datetime.date.today() - datetime.timedelta(days=90)
    sql_update = 'SELECT ' \
                 'po.idpost, po.idperfil, pe.idempresa, po.nomeimagem, po.qtdretweets, po.postagem, po.qtdfavoritado, po.datahora ' \
                 'FROM tw_posts po INNER JOIN tw_perfis pe ON po.idperfil = pe.idperfil ' \
                 'WHERE po.idpost <= (%s) AND datahora >= (%s)'

    cur.execute(sql_update, (ultimo_post, since_date))
    results = cur.fetchall()

    importa_twitter(results)


def importa_twitter(results):
    for post in results:
        postelast = {
            'idpost': post['idpost'],
            'idperfil': post['idperfil'],
            'idempresa': post['idempresa'],
            'postagem': post['postagem'],
            'curtidas': post['qtdfavoritado'],
            'retweets': post['qtdretweets'],
            'nomeimagem': post['nomeimagem'],
            'datahora': post['datahora'].strftime("%d/%m/%y %H:%M:%S")
        }

        sql_nomeempresa = 'SELECT nome FROM empresas WHERE idempresa = (%s)'
        cur.execute(sql_nomeempresa, post['idempresa'])
        nomeempresa = cur.fetchone()['nome']
        postelast['nomeempresa'] = nomeempresa

        sqlpromo = 'SELECT promo.idpromocao, promo.nome ' \
                   'FROM promo_tw_posts ptp INNER JOIN promo_promocoes promo ON ptp.idpromocao = promo.idpromocao ' \
                   'WHERE idpost = (%s)'

        cur.execute(sqlpromo, post['idpost'])
        promos = cur.fetchall()
        promos_json = []
        for promo in promos:
            promos_json.append({'idpromocao': promo["idpromocao"], 'nome': promo["nome"]})

        postelast['promocoes'] = promos_json

        es.index(index=indice + "tw_posts", doc_type='tw_post', body=postelast, id=postelast["idpost"])


def insta_importador():
    res = es.search(index=indice + "insta_posts", doc_type='insta_post', body=query)

    if len(res['hits']['hits']) > 0:
        ultimo_post = res['hits']['hits'][0]["_source"]["idpost"]
    else:
        ultimo_post = 0

    sql = 'SELECT ' \
          'po.idpost, po.idperfil, pe.idempresa, po.nomeimagem, po.qtdcomentarios, po.postagem, po.qtdcurtidas, po.datahora ' \
          'FROM insta_posts po INNER JOIN insta_perfis pe ON po.idperfil = pe.idperfil ' \
          'WHERE po.idpost > (%s)'

    cur.execute(sql, ultimo_post)
    results = cur.fetchall()

    importa_insta(results)

    since_date = datetime.date.today() - datetime.timedelta(days=90)
    sql_update = 'SELECT ' \
                 'po.idpost, po.idperfil, pe.idempresa, po.nomeimagem, po.qtdcomentarios, po.postagem, po.qtdcurtidas, po.datahora ' \
                 'FROM insta_posts po INNER JOIN insta_perfis pe ON po.idperfil = pe.idperfil ' \
                 'WHERE po.idpost <= (%s) AND datahora >= (%s)'

    cur.execute(sql_update, (ultimo_post, since_date))
    results = cur.fetchall()

    importa_insta(results)


def importa_insta(results):
    for post in results:
        postelast = {
            'idpost': post['idpost'],
            'idperfil': post['idperfil'],
            'idempresa': post['idempresa'],
            'postagem': post['postagem'],
            'curtidas': post['qtdcurtidas'],
            'qtdcomentarios': post['qtdcomentarios'],
            'nomeimagem': post['nomeimagem'],
            'datahora': post['datahora'].strftime("%d/%m/%y %H:%M:%S")
        }

        sql_nomeempresa = 'SELECT nome FROM empresas WHERE idempresa = (%s)'
        cur.execute(sql_nomeempresa, post['idempresa'])
        nomeempresa = cur.fetchone()['nome']
        postelast['nomeempresa'] = nomeempresa

        sqlpromo = 'SELECT promo.idpromocao, promo.nome ' \
                   'FROM promo_insta_posts pip INNER JOIN promo_promocoes promo ON pip.idpromocao = promo.idpromocao ' \
                   'WHERE idpost = (%s)'

        cur.execute(sqlpromo, post['idpost'])
        promos = cur.fetchall()
        promos_json = []
        for promo in promos:
            promos_json.append({'idpromocao': promo["idpromocao"], 'nome': promo["nome"]})

        postelast['promocoes'] = promos_json

        sql_post_comentarios = 'SELECT * FROM insta_comentarios WHERE idpost = (%s)'
        cur.execute(sql_post_comentarios, post['idpost'])
        comentarios = cur.fetchall()
        comentarios_json = []

        for comentario in comentarios:
            # noinspection PyTypeChecker
            comentario_json = {
                'postagem': comentario['postagem'],
                'datahora': comentario['datahora'].strftime("%d/%m/%y %H:%M:%S"),
                'nomeusuario': comentario['nomeusuario']
            }
            comentarios_json.append(comentario_json)

        postelast['comentarios'] = comentarios_json

        es.index(index=indice + "insta_posts", doc_type='insta_post', body=postelast, id=postelast["idpost"])


def yt_importador():
    query_yt = {
        "query": {
            "match_all": {

            }
        },
        "size": 1,
        "sort": [
            {
                "idvideo": {
                    "order": "desc"
                }
            }
        ]
    }

    res = es.search(index=indice + "yt_videos", doc_type='yt_video', body=query_yt)

    if len(res['hits']['hits']) > 0:
        ultimo_post = res['hits']['hits'][0]["_source"]["idvideo"]
    else:
        ultimo_post = 0

    sql = 'SELECT ' \
          'po.idvideo, po.idcanal, pe.idempresa, po.titulo, po.nomeimagem, po.qtdvisualizacoes, po.qtddescurtidas, po.qtdcomentarios, ' \
          'po.descricao, po.qtdcurtidas, po.datahora ' \
          'FROM yt_videos po INNER JOIN yt_canais pe ON po.idcanal = pe.idcanal ' \
          'WHERE po.idvideo > (%s)'

    cur.execute(sql, ultimo_post)
    results = cur.fetchall()

    importa_yt(results)

    since_date = datetime.date.today() - datetime.timedelta(days=90)
    sql_update = 'SELECT ' \
                 'po.idvideo, po.idcanal, pe.idempresa, po.titulo, po.nomeimagem, po.qtdvisualizacoes, po.qtddescurtidas, po.qtdcomentarios, ' \
                 'po.descricao, po.qtdcurtidas, po.datahora ' \
                 'FROM yt_videos po INNER JOIN yt_canais pe ON po.idcanal = pe.idcanal ' \
                 'WHERE po.idvideo <= (%s) AND datahora >= (%s)'

    cur.execute(sql_update, (ultimo_post, since_date))
    results = cur.fetchall()

    importa_yt(results)


def importa_yt(results):
    for post in results:
        postelast = {
            'idvideo': post['idvideo'],
            'idcanal': post['idcanal'],
            'idempresa': post['idempresa'],
            'descricao': post['titulo'] + '\n' + post['descricao'],
            'curtidas': post['qtdcurtidas'],
            'qtdcomentarios': post['qtdcomentarios'],
            'visualizacoes': post['qtdvisualizacoes'],
            'descurtidas': post['qtddescurtidas'],
            'nomeimagem': post['nomeimagem'],
            'datahora': post['datahora'].strftime("%d/%m/%y %H:%M:%S")
        }

        sql_nomeempresa = 'SELECT nome FROM empresas WHERE idempresa = (%s)'
        cur.execute(sql_nomeempresa, post['idempresa'])
        nomeempresa = cur.fetchone()['nome']
        postelast['nomeempresa'] = nomeempresa

        sqlpromo = 'SELECT promo.idpromocao, promo.nome ' \
                   'FROM promo_yt_videos pyv INNER JOIN promo_promocoes promo ON pyv.idpromocao = promo.idpromocao ' \
                   'WHERE idvideo = (%s)'

        cur.execute(sqlpromo, post['idvideo'])
        promos = cur.fetchall()
        promos_json = []
        for promo in promos:
            promos_json.append({'idpromocao': promo["idpromocao"], 'nome': promo["nome"]})

        postelast['promocoes'] = promos_json

        sql_post_comentarios = 'SELECT * FROM yt_comentarios WHERE idvideo = (%s)'
        cur.execute(sql_post_comentarios, post['idvideo'])
        comentarios = cur.fetchall()
        comentarios_json = []

        comentarios_primeira_ordem = [com for com in comentarios if com['idcomentarioresposta'] is None]
        for comentario in comentarios_primeira_ordem:
            # noinspection PyTypeChecker
            comentario_json = {
                'postagem': comentario['postagem'],
                'curtidas': comentario['qtdcurtidas'],
                'datahora': comentario['datahora'].strftime("%d/%m/%y %H:%M:%S"),
                'nomeusuario': comentario['nomeusuario']
            }
            respostas_json = []
            # noinspection PyTypeChecker
            respostas_comentario = [resp for resp in comentarios if
                                    resp['idcomentarioresposta'] == comentario['idcomentario']]
            for resposta in respostas_comentario:
                # noinspection PyTypeChecker
                resposta_json = {
                    'postagem': resposta['postagem'],
                    'curtidas': resposta['qtdcurtidas'],
                    'datahora': resposta['datahora'].strftime("%d/%m/%y %H:%M:%S"),
                    'nomeusuario': resposta['nomeusuario']
                }
                respostas_json.append(resposta_json)
            comentario_json['respostas'] = respostas_json
            comentarios_json.append(comentario_json)

        postelast['comentarios'] = comentarios_json

        es.index(index=indice + "yt_videos", doc_type='yt_video', body=postelast, id=postelast["idvideo"])


fb_importador()
tw_importador()
insta_importador()
yt_importador()

logging.warning("Tempo de execução do script: %s segundos" % (time.time() - start_time))
logging.warning("Script executado com sucesso!")
