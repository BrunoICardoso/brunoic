import banco
import pymysql
from elasticsearch import Elasticsearch

es = Elasticsearch(['172.16.61.3:9200'])
conn, cur = banco.conecta_banco()

query = {
    "query": {
        "match_all": {

        }
    },
    "size":1,
    "sort": [
    {
      "idnoticia": {
        "order": "desc"
      }
    }
    ]
}

res = es.search(index="zeeng", doc_type='noticias', body=query)

if(len(res['hits']['hits']) > 0):
    ultimaNoticia = res['hits']['hits'][0]["_source"]["idnoticia"]
else:
    ultimaNoticia = 0

print(ultimaNoticia)

sqlimagens = 'SELECT ' \
          'url, titulo, creditos ' \
          'FROM ' \
          'noticia_imagens ' \
          ' WHERE ' \
          'idnoticia = (%s)'

sqlempresas = 'SELECT ' \
                'ne.idnoticiaempresa, ' \
                'ne.idempresa, ' \
                'e.nome as nomeempresa, ' \
                'e.descricao as descricaoempresa ' \
                'from ' \
                'noticias_empresa ne inner join ' \
                'empresas e on ne.idempresa = e.idempresa ' \
                'where ' \
                'ne.idnoticia = (%s)'

sqltermos = 'SELECT ' \
                'termo, ' \
                'frequencia '\
                'from ' \
                'noticia_termos '\
                'where ' \
                'idnoticia = %s'

sql ='SELECT '\
            'n.idnoticia, n.idfonte, n.idnoticia_knewin, ' \
            'n.titulo, n.subtitulo, n.dominio, ' \
            'n.autor, n.conteudo, n.url, ' \
            'n.datapublicacao, n.datacaptura_zeeng, ' \
            'n.categoria, n.localidade, n.linguagem, ' \
            'f.nome as nomefonte ' \
        'FROM ' \
            'noticias n inner join ' \
            'fontes_noticias f ON n.idfonte = f.idfonte ' \
        ' WHERE ' \
            'n.idnoticia > (%s) LIMIT 1000;'

print(sql)

cur.execute(sql, ultimaNoticia)
results = cur.fetchall()

print(len(results))
for noticia in results:
    # print(noticia["datapublicacao"].strftime("%d/%m/%y %H:%M:%S"))
    print(noticia["idnoticia"])

    noticiaelast = {
         'idnoticia': noticia["idnoticia"],
         'idfonte': noticia["idfonte"],
         'nomefonte': noticia["nomefonte"],
         'titulo': noticia["titulo"],
         'subtitulo': noticia["subtitulo"],
         'dominio':noticia["dominio"],
         'autor':noticia["autor"],
         'conteudo':noticia["conteudo"],
         'url':noticia["url"],
         'datapublicacao': None if noticia["datapublicacao"] is None else noticia["datapublicacao"].strftime("%d/%m/%y %H:%M:%S"),
         #'datacaptura_knewin':noticia["datacaptura_knewin"].strftime("%d/%m/%y %H:%M:%S"),
         'datacaptura_zeeng': None if noticia["datacaptura_zeeng"] is None else noticia["datacaptura_zeeng"].strftime("%d/%m/%y %H:%M:%S"),
         'categoria':noticia["categoria"],
         'localidade':noticia["localidade"],
         'linguagem':noticia["linguagem"]
    }

    cur.execute(sqlimagens, noticiaelast["idnoticia"])
    imagensBanco = cur.fetchall()
    imagens = []
    for img in imagensBanco:
        imagens.append({'titulo': img["titulo"], 'url': img["url"], 'creditos': img["creditos"]})

    cur.execute(sqlempresas, noticiaelast["idnoticia"])
    empresasBanco = cur.fetchall()
    empresasNoticia = []
    for empNoticia in empresasBanco:
        empresasNoticia.append({'idnoticiaempresa': empNoticia["idnoticiaempresa"],
                                'idempresa': empNoticia["idempresa"],
                                'nomeempresa': empNoticia["nomeempresa"],
                                'descricaoempresa': empNoticia["descricaoempresa"],})

    cur.execute(sqltermos, noticiaelast["idnoticia"])
    termosBanco = cur.fetchall()
    termosNoticia = []
    # for termo in termosBanco:
    #     termosNoticia.append({'termo': termo["termo"],
    #                             'frequencia': termo["frequencia"]})

    noticiaelast["imagens"] = imagens
    noticiaelast["empresas"] = empresasNoticia
    # noticiaelast["termos"] = termosNoticia;

    res = es.index(index="zeeng", doc_type='noticias', body=noticiaelast, id=noticiaelast["idnoticia"])
    # print(res['created'])
