import logging

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
            "idpromocao": {
                "order": "desc"
            }
        }
    ]
}

start_time = time.time()


def promo_importador():
    sql = 'SELECT * FROM promo_promocoes'

    cur.execute(sql)
    results = cur.fetchall()
    print(len(results))

    for promo in results:
        jsonpromo = {
            'idpromocao': promo['idpromocao'],
            'nomepromocao': promo['nome'],
            'idmodalidade': promo['idmodalidade'],
            'idorgaoregulador': promo['idorgaoregulador'],
            'outrosinteressados': promo['outrosinteressados'],
            'certificadoautorizacao': promo['certificadoautorizacao'],
            'dtcadastro': promo['dtcadastro'].strftime("%Y-%m-%d") if promo['dtcadastro'] else None,
            'dtvigenciaini': promo['dtvigenciaini'].strftime("%Y-%m-%d") if promo['dtvigenciaini'] else None,
            'dtvigenciafim': promo['dtvigenciafim'].strftime("%Y-%m-%d") if promo['dtvigenciafim'] else None,
            'valorpremios': promo['valorpremios'],
            'linksitepromocao': promo['linksitepromocao'],
            'linkfacebook': promo['linkfacebook'],
            'linktwitter': promo['linktwitter'],
            'linkinstagram': promo['linkinstagram'],
            'linkyoutube': promo['linkyoutube'],
            'mecanicapromo': promo['mecanicapromo'],
            'produtosparticipantes': promo['produtosparticipantes'],
            'premiospromo': promo['premiospromo'],
            'linkregulamento': promo['linkregulamento'],
            'textoregulamento': promo['textoregulamento'],
            'abrangencia_nacional': bool(ord(promo["abrangencianacional"])) if promo['abrangencianacional'] else False
        }

        sql_promo_empresas = 'SELECT p.idempresa, nome FROM promo_promoempresas p INNER JOIN empresas e ' \
                             'ON p.idempresa = e.idempresa WHERE idpromocao = (%s)'
        cur.execute(sql_promo_empresas, promo['idpromocao'])
        empresas = cur.fetchall()
        empresas_json = []

        for empresa in empresas:
            empresas_json.append({'idempresa': empresa["idempresa"], 'nome': empresa["nome"]})

        jsonpromo['empresas'] = empresas_json

        sql_promo_abrangestados = 'SELECT e.idestado, nome, uf FROM promo_estadosabrangencia p INNER JOIN estados e ' \
                                  'ON p.idestado = e.idestado WHERE idpromocao = (%s)'
        cur.execute(sql_promo_abrangestados, promo['idpromocao'])
        estados = cur.fetchall()
        estados_json = []

        for estado in estados:
            estados_json.append({'idestado': estado['idestado'], 'nome': estado['nome'], 'uf': estado['uf']})

        jsonpromo['abrangestados'] = estados_json

        sql_promo_abrangmunicipios = 'SELECT m.idmunicipio, m.nome, m.idestado, uf FROM promo_municipiosabrangencia p ' \
                                     'INNER JOIN municipios m ON p.idmunicipio = m.idmunicipio ' \
                                     'INNER JOIN estados e ON m.idestado = e.idestado ' \
                                     'WHERE idpromocao = (%s)'
        cur.execute(sql_promo_abrangmunicipios, promo['idpromocao'])
        municipios = cur.fetchall()
        municipios_json = []

        for municipio in municipios:
            municipios_json.append({'idmunicipio': municipio['idmunicipio'], 'idestado': municipio['idestado'],
                                    'nome': municipio['nome'], 'uf': municipio['uf']})

        jsonpromo['abrangmunicipios'] = municipios_json

        sql_promo_arquivosregulamento = 'SELECT * FROM promo_regulamentoarquivos WHERE idpromocao = (%s)'
        cur.execute(sql_promo_arquivosregulamento, promo['idpromocao'])
        arquivos_regulamento = cur.fetchall()
        arquivos_regulamento_json = []
        for arquivo in arquivos_regulamento:
            arquivos_regulamento_json.append({'nomearquivo': arquivo['nome'], 'tipo': arquivo['tipo']})

        jsonpromo['arquivosregulamento'] = arquivos_regulamento_json

        sql_promo_arquivosrelacionados = 'SELECT * FROM promo_arquivos WHERE idpromocao = (%s)'
        cur.execute(sql_promo_arquivosrelacionados, promo['idpromocao'])
        arquivos_relacionados = cur.fetchall()
        arquivos_relacionados_json = []
        for arquivo in arquivos_relacionados:
            arquivos_relacionados_json.append({'nomearquivo': arquivo['nome'], 'tipo': arquivo['tipo'], 'url': arquivo['url']})

        jsonpromo['arquivosrelacionados'] = arquivos_relacionados_json

        if promo['idmodalidade']:
            sql_modalidade = 'SELECT nome FROM promo_modalidades WHERE idpromomodalidade = (%s)'
            cur.execute(sql_modalidade, promo['idmodalidade'])
            modalidade = cur.fetchone()
            jsonpromo['nomemodalidade'] = modalidade['nome']

        if promo['idorgaoregulador']:
            sql_orgaoregulador = 'SELECT nome FROM promo_orgaosreguladores WHERE idorgao = (%s)'
            cur.execute(sql_orgaoregulador, promo['idorgaoregulador'])
            orgaoregulador = cur.fetchone()
            jsonpromo['nomeorgaoregulador'] = orgaoregulador['nome']

        jsonpromo['noticias'] = get_json_noticias(promo['idpromocao'])
        jsonpromo['postsfacebook'] = get_json_facebook(promo['idpromocao'])
        jsonpromo['poststwitter'] = get_json_twitter(promo['idpromocao'])
        jsonpromo['postsinstagram'] = get_json_instagram(promo['idpromocao'])
        jsonpromo['videosyoutube'] = get_json_youtube(promo['idpromocao'])

        es.index(index=indice + "promocoes", doc_type='promocao', body=jsonpromo, id=jsonpromo["idpromocao"])


def get_json_noticias(idpromocao):
    noticias_json = []
    sql_get_noticias_promo = 'SELECT * FROM promo_promonoticias p INNER JOIN noticias n ' \
                             'ON p.idnoticia = n.idnoticia INNER JOIN fontes_noticias f ON n.idfonte = f.idfonte ' \
                             'WHERE idpromocao = (%s)'
    cur.execute(sql_get_noticias_promo, idpromocao)
    noticias = cur.fetchall()
    for noticia in noticias:
        noticia_json = {
            'autor': noticia['autor'],
            'conteudo': noticia['conteudo'],
            'datapublicacao': noticia['datapublicacao'].strftime("%Y-%m-%d"),
            'idnoticia': noticia['idnoticia'],
            'link': noticia['url'],
            'nomefonte': noticia['nome'],
            'titulo': noticia['titulo']
        }
        noticias_json.append(noticia_json)

    return noticias_json


def get_json_facebook(idpromocao):
    posts_fb_json = []
    sql_get_fb_posts = 'SELECT * FROM promo_fb_posts p INNER JOIN fb_posts f ON p.idpost = f.idpost ' \
                       'WHERE idpromocao = (%s)'
    cur.execute(sql_get_fb_posts, idpromocao)
    fb_posts = cur.fetchall()

    for fb_post in fb_posts:
        fb_post_json = {
            'compartilhamentos': fb_post['compartilhamentos'],
            'curtidas': fb_post['reacoes'],
            'qtdcomentarios': fb_post['comentarios'],
            'postagem': fb_post['postagem'],
            'idpost': fb_post['idpost'],
            'datahora': fb_post['datahora'].strftime("%d/%m/%y %H:%M:%S"),
            'nomeimagem': fb_post['nomeimagem'],
        }

        posts_fb_json.append(fb_post_json)

    return posts_fb_json


def get_json_twitter(idpromocao):
    posts_tw_json = []
    sql_get_tw_posts = 'SELECT * FROM promo_tw_posts p INNER JOIN tw_posts t ON p.idpost = t.idpost ' \
                       'WHERE idpromocao = (%s)'
    cur.execute(sql_get_tw_posts, idpromocao)
    tw_posts = cur.fetchall()

    for tw_post in tw_posts:
        tw_post_json = {
            'retweets': tw_post['qtdretweets'],
            'curtidas': tw_post['qtdfavoritado'],
            'postagem': tw_post['postagem'],
            'idpost': tw_post['idpost'],
            'datahora': tw_post['datahora'].strftime("%d/%m/%y %H:%M:%S"),
            'nomeimagem': tw_post['nomeimagem'],
        }

        posts_tw_json.append(tw_post_json)

    return posts_tw_json


def get_json_instagram(idpromocao):
    posts_insta_json = []
    sql_get_insta_posts = 'SELECT * FROM promo_insta_posts p INNER JOIN insta_posts i ON p.idpost = i.idpost ' \
                          'WHERE idpromocao = (%s)'
    cur.execute(sql_get_insta_posts, idpromocao)
    insta_posts = cur.fetchall()

    for insta_post in insta_posts:
        insta_post_json = {
            'curtidas': insta_post['qtdcurtidas'],
            'qtdcomentarios': insta_post['qtdcomentarios'],
            'postagem': insta_post['postagem'],
            'idpost': insta_post['idpost'],
            'datahora': insta_post['datahora'].strftime("%d/%m/%y %H:%M:%S"),
            'nomeimagem': insta_post['nomeimagem']
        }

        posts_insta_json.append(insta_post_json)

    return posts_insta_json


def get_json_youtube(idpromocao):
    videos_yt_json = []
    sql_get_yt_videos = 'SELECT * FROM promo_yt_videos p INNER JOIN yt_videos y ON p.idvideo = y.idvideo ' \
                        'WHERE idpromocao = (%s)'
    cur.execute(sql_get_yt_videos, idpromocao)
    yt_videos = cur.fetchall()

    for yt_video in yt_videos:
        yt_video_json = {
            'visualizacoes': yt_video['qtdvisualizacoes'],
            'curtidas': yt_video['qtdcurtidas'],
            'descurtidas': yt_video['qtddescurtidas'],
            'qtdcomentarios': yt_video['qtdcomentarios'],
            'descricao': yt_video['descricao'],
            'idvideo': yt_video['idvideo'],
            'datahora': yt_video['datahora'].strftime("%d/%m/%y %H:%M:%S"),
            'nomeimagem': yt_video['nomeimagem'],
        }

        videos_yt_json.append(yt_video_json)

    return videos_yt_json


promo_importador()
logging.warning("Tempo de execução do script: %s segundos" % (time.time() - start_time))
logging.warning("Script executado com sucesso!")
