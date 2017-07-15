import logging
import json
import datetime
import time


import pymysql
import banco
from safe_requests import post_request_and_jsonize
MAXTAMANHOCONTEUDO = 11000
MAXAUTOR = 120
MAXSUBTITULO = 800
MAXTITULO = 500
MAXCATEGORIA = 100

conn, cur = banco.conecta_banco()

url_captura = 'http://data.knewin.com/news'
key = '2e9ee79e-7a4f-4f2f-bca3-25402e113997'

sql_fontes = 'SELECT `idfonte_knewin` FROM fontes_noticias ' \
             'WHERE `ativo` = 1 AND `excluido` = 0 AND `manual` = 0'
cur.execute(sql_fontes)
fontes = cur.fetchall()
lista_fontes = []

for fonte in fontes:
    lista_fontes.append(fonte['idfonte_knewin'])

json_captura = {
    "key": key,
    "query": str,
    "offset": int,
    "sort": {
        "field": "crawled_date",
        "order": "asc"
    },
    "filter": {
        "language": ["pt"],
        "sourceId": lista_fontes,
        "sinceCrawled": str
    }
}

sql_noticias = 'INSERT INTO noticias (`idfonte`, `idnoticia_knewin`, `titulo`, `subtitulo`, `dominio`,' \
               '`autor`, `conteudo`, `url`, `datapublicacao`, `datacaptura_knewin`, `datacaptura_zeeng`,' \
               '`categoria`, `localidade`, `linguagem`)' \
               'SELECT idfonte, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s' \
               'FROM fontes_noticias where idfonte_knewin = %s'

sql_noticias_empresa = 'INSERT INTO noticias_empresa (`idempresa`, `idnoticia`) VALUES (%s, LAST_INSERT_ID())'

sql_noticias_empresa2 = 'INSERT INTO noticias_empresa (`idempresa`, `idnoticia`) VALUES (%s, %s)'

sql_update1 = 'UPDATE empresa_expressoes_noticias SET `offsetcaptura` = 0, `dataultimacaptura` = (%s) ' \
              'WHERE `idexpressoesnoticias` = (%s)'

sql_update2 = 'UPDATE empresa_expressoes_noticias SET `offsetcaptura` = (%s) WHERE `idexpressoesnoticias` = (%s)'

sql_get_expressoes = 'SELECT * FROM empresa_expressoes_noticias'

sql_check = 'SELECT * FROM noticias WHERE `idnoticia_knewin` = (%s)'

sql_get_id = 'SELECT `idnoticia` FROM noticias WHERE `idnoticia_knewin` = (%s)'

sql_insert_image = 'INSERT INTO noticia_imagens (`idnoticia`, `url`, `titulo`, `creditos`) VALUES (%s, %s, %s, %s)'


def get_all_noticias_empresas():
    logging.info('Iniciando a captura de notícias de todas as expressões para empresas.')
    cur.execute(sql_get_expressoes)
    results = cur.fetchall()

    for result in results:
        get_noticias_empresas(result)


def get_imagens(noticia):
    cur.execute(sql_get_id, noticia['id'])
    idnoticia = cur.fetchone()['idnoticia']
    imagens = noticia['image_hits']
    for imagem in imagens:
        url = imagem['url']
        titulo = imagem['caption'] if 'caption' in imagem else None
        creditos = imagem['credit'] if 'credit' in imagem else None
        try:
            cur.execute(sql_insert_image, (idnoticia, url, titulo, creditos))
        except Exception as excpt:
            logging.error('%s', excpt)
    conn.commit()


def get_noticias_empresas(result):
    json_captura['query'] = result['expressao']
    offset = json_captura['offset'] = result['offsetcaptura']
    json_captura['filter']['sinceCrawled'] = result['dataultimacaptura'].date().strftime('%Y-%m-%dT00:00:00')\
        if result['dataultimacaptura'] else \
        (datetime.datetime.today() - datetime.timedelta(days=90)).strftime('%Y-%m-%dT00:00:00')
    idexpressoesnoticias = result['idexpressoesnoticias']
    idempresa = result['idempresa']

    count = 0
    while True:
        noticias = post_request_and_jsonize(url_captura, json.dumps(json_captura))
        if noticias['count'] == 0:
            if count == 0:
                logging.info('Nenhuma nova notícia para a expressão %d', idexpressoesnoticias)
                return
            else:
                logging.info('Últimas %d notícias da expressao %d capturadas.', count, idexpressoesnoticias)
                break

        for noticia in noticias['hits']:
            datacaptura_knewin = datetime.datetime.strptime(noticia['crawled_date'], '%Y-%m-%dT%H:%M:%S+0000')
            idfonte_knewin = noticia['source_id']
            idnoticia_knewin = noticia['id']

            cur.execute(sql_check, idnoticia_knewin)
            existe = cur.fetchone()
            if existe:
                logging.info('Notícia %d já existe. Verificando se está vinculada à mesma empresa.',
                             existe['idnoticia'])
                try:
                    cur.execute(sql_noticias_empresa2, (idempresa, existe['idnoticia']))
                    logging.info('Notícia existente vinculada a outra empresa. '
                                 'Novo registro adicionado à tabela noticias_empresa.')
                    count += 1
                    offset += 1
                    continue
                except pymysql.err.IntegrityError:
                    logging.error('Notícia %d já está vinculada à empresa %d.', existe['idnoticia'], idempresa)
                    offset += 1
                    cur.execute(sql_update2, (offset, idexpressoesnoticias))
                    continue

            if 'title' in noticia:
                titulo = noticia['title'] if len(noticia['title']) <= MAXTITULO else noticia['title'][:MAXTITULO]
            else:
                titulo = None
            if 'subtitle' in noticia:
                subtitulo = noticia['subtitle'] \
                    if len(noticia['subtitle']) < MAXSUBTITULO else noticia['subtitle'][:MAXSUBTITULO]
            else:
                subtitulo = None
            dominio = noticia['domain']
            autor = None
            if 'author' in noticia:
                autor = noticia['author'] if len(noticia['author']) <= MAXAUTOR else noticia['author'][:MAXAUTOR]
            if 'content' in noticia:
                if len(noticia['content']) < 200:
                    offset += 1
                    continue
                conteudo = noticia['content'] if len(noticia['content']) < MAXTAMANHOCONTEUDO\
                    else noticia['content'][:MAXTAMANHOCONTEUDO]
            else:
                conteudo = None
            url = noticia['url']

            datapublicacao = datetime.datetime.strptime(noticia['published_date'], '%Y-%m-%dT%H:%M:%S') \
                if 'published_date' in noticia else None
            if 'published_date' in noticia:
                if (datacaptura_knewin - datetime.timedelta(days=15)) > datapublicacao:
                    offset += 1
                    continue

            datacaptura_zeeng = datetime.datetime.utcnow()
            if 'category' in noticia:
                categoria = noticia['category'] if len(noticia['category']) <= MAXCATEGORIA\
                    else noticia['category'][:MAXCATEGORIA]
            else:
                categoria = None

            localidade = noticia['source_locality'][0]['country'] if 'source_locality' in noticia else None
            linguagem = noticia['lang']
            try:
                cur.execute(sql_noticias,
                            (idnoticia_knewin, titulo, subtitulo, dominio, autor, conteudo, url, datapublicacao,
                             datacaptura_knewin, datacaptura_zeeng, categoria, localidade, linguagem,
                             idfonte_knewin))
                cur.execute(sql_noticias_empresa, idempresa)
                count += 1
                offset += 1
            except Exception as excpt:
                logging.error('%s', excpt)

            if 'image_hits' in noticia:
                get_imagens(noticia)

        logging.info('Capturadas as últimas %d notícias da expressão de id %d', count, idexpressoesnoticias)
        conn.commit()
        json_captura['offset'] = offset

    dataultimacaptura = datacaptura_knewin.date()
    if dataultimacaptura != result['dataultimacaptura'].date():
        cur.execute(sql_update1, (datacaptura_knewin, idexpressoesnoticias))
        conn.commit()
    else:
        cur.execute(sql_update2, (offset, idexpressoesnoticias))
        conn.commit()


if __name__ == "__main__":
    start_time = time.time()
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    get_all_noticias_empresas()
    cur.close()
    conn.close()
    logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
    logging.info("Script executado com sucesso!")

