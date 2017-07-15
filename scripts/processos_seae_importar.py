import banco
import pymysql
from elasticsearch import Elasticsearch

# es = Elasticsearch(['172.16.39.5:9200'])
# es = Elasticsearch(['172.16.0.25:9200'])
# es = Elasticsearch(['172.16.0.85:9200'])
es = Elasticsearch(['187.103.103.11:9200'])

conn, cur = banco.conecta_banco()

# query = {
#     "query": {
#         "match_all": {
#
#         }
#     },
#     "size":1,
#     "sort": [
#     {
#       "idprocesso": {
#         "order": "desc"
#       }
#     }
#     ]
# }
#
# res = es.search(index="zeeng", doc_type='processos_seae', body=query)

# if(len(res['hits']['hits']) > 0):
#     ultimaNoticia = res['hits']['hits'][0]["_source"]["idnoticia"]
# else:
#     ultimaNoticia = 0
#
# print(ultimaNoticia)

sqlestados = 'Select e.idestado, e.nome, e.uf ' \
             'From  ' \
		        'seae_abrang_estado abrang inner join ' \
                'estados e on abrang.idestado = e.idestado ' \
             'where  ' \
                'abrang.idprocesso = (%s);' \


sqlmunicipios = 'Select m.idmunicipio, m.nome as nomemunicipio, e.nome as nomeestado, e.uf ' \
                'From  ' \
                    'seae_abrang_municipio abrang inner join ' \
                    'municipios m on abrang.idmunicipio = m.idmunicipio inner join ' \
                    'estados e on m.idestado = e.idestado '\
                'where  ' \
                    'abrang.idprocesso = (%s); '\


sqlsetores = 'Select ' \
                    's.idsetor, ' \
                    's.codsetor, ' \
                    's.descricao as descsetor, ' \
                    'ss.codsubsetor, ' \
                    'ss.descricao as descsubsetor  ' \
	         'From ' \
		            'seae_processo_setor ps inner join  ' \
		            'seae_setores_proc s on ps.idsetor = s.idsetor inner join  ' \
                    'seae_subsetores_proc ss on ps.idsubsetor = ss.idsubsetor  ' \
	         'where ' \
		            'ps.idprocesso = (%s)'


sqlarquivos = 'Select ' \
                    'a.idarquivo, a.numdoc, a.coordenacao, ' \
                    'a.situacao, a.link, a.nomearquivo, a.textoarquivo ' \
              'from ' \
                    'seae_arquivos_proc a ' \
              'where ' \
                    'a.idprocesso = (%s);'

sqlsolicitantes = 'Select ' \
	                    's.idsolicitante, s.solicitante, s.cnpj ' \
                    'From  ' \
	                    'seae_processo_solicitantes s ' \
                    'where ' \
	                    'idprocesso = (%s);'

sqlempresas = 'Select  ' \
	                'e.idempresa, e.nome ' \
                'from  ' \
                    'seae_empresa_processos ep inner join ' \
                    'empresas e on ep.idempresa = e.idempresa ' \
                'where ' \
	                'ep.idprocesso = (%s); '


sql ='Select ' \
     'p.idprocesso, ' \
     'p.numprocesso, ' \
     'p.dtprocesso, ' \
     'p.dtvigenciaini,  ' \
     'p.dtvigenciafim,  ' \
     'p.interessados, ' \
     'p.premios, ' \
     'p.valortotalpremios, ' \
     'p.modalidade, ' \
     'p.formacontemplacao, ' \
     'p.abrangencia_nacional, ' \
     'p.numdocs,  ' \
     'p.nome, ' \
     'p.resumo, ' \
     'p.comoparticipar,' \
     'p.excluido, ' \
     '' \
     '(select s.descricao ' \
     '		from seae_mov_situacao mov inner join ' \
     '      seae_situacoes s on mov.idsituacao = s.idsituacao ' \
     '   where ' \
     '      mov.idprocesso = p.idprocesso ' \
     '   order by ' \
     '      mov.dtsituacao desc limit 1) as situacaoAtual, ' \
     '' \
     '(select mov.dtsituacao ' \
     '		from seae_mov_situacao mov inner join ' \
     '      seae_situacoes s on mov.idsituacao = s.idsituacao ' \
     '   where ' \
     '      mov.idprocesso = p.idprocesso ' \
     '   order by ' \
     '      mov.dtsituacao desc limit 1) as datasituacaoatual, ' \
     '' \
     '(select s.idsituacao ' \
     '		from seae_mov_situacao mov inner join ' \
     '      seae_situacoes s on mov.idsituacao = s.idsituacao ' \
     '   where ' \
     '      mov.idprocesso = p.idprocesso ' \
     '   order by ' \
     '      mov.dtsituacao desc limit 1) as idsituacaoatual ' \
     'from  ' \
     ' 	    seae_processos p ' \
     '  where  ' \
     '  p.atualizarElastic = true '


print(sql);

# cur.execute(sql, ultimaNoticia)
cur.execute(sql)
results = cur.fetchall()

print(len(results))
for processo in results:
    # print(noticia["datapublicacao"].strftime("%d/%m/%y %H:%M:%S"))
    print(processo["idprocesso"])

    if(bool(ord(processo["excluido"]))):
        es.delete(index='promocoes', doc_type='processos_seae', id=processo["idprocesso"])
    else:
        processoelastic = {
             'idprocesso': processo["idprocesso"],
             'numprocesso': processo["numprocesso"],
             'dtprocesso': None if processo["dtprocesso"] is None else processo["dtprocesso"].strftime("%Y-%m-%d"),
             'dtvigenciaini': None if processo["dtvigenciaini"] is None else processo["dtvigenciaini"].strftime("%Y-%m-%d"),
             'dtvigenciafim': None if processo["dtvigenciafim"] is None else processo["dtvigenciafim"].strftime("%Y-%m-%d"),
             'interessados': processo["interessados"],
             'premios': processo["premios"],
             'valortotalpremios':processo["valortotalpremios"],
             'modalidade':processo["modalidade"],
             'formacontemplacao':processo["formacontemplacao"],
             'abrangencia_nacional':bool(ord(processo["abrangencia_nacional"])),
             'numdocs':processo["numdocs"],
             'nome':processo["nome"],
             'resumo': processo["resumo"],
             'comoparticipar':processo["comoparticipar"],
             'situacaoatual': processo["situacaoAtual"],
             'datasituacaoatual': None if processo["datasituacaoatual"] is None else processo["datasituacaoatual"].strftime("%Y-%m-%d"),
             'idsituacaoatual': processo["idsituacaoatual"]

        }
        print(processoelastic)

        # Carrega os estados de abrangência do processo
        cur.execute(sqlestados, processoelastic["idprocesso"])
        estadosBanco = cur.fetchall()
        estados = []
        for estado in estadosBanco:
            estados.append({'idestado':estado["idestado"], 'nome':estado["nome"], 'uf':estado["uf"]})

        # Carrega os municípios de abrangência do processo
        cur.execute(sqlmunicipios, processoelastic["idprocesso"])
        municipiosBanco = cur.fetchall()
        municipios = []
        for municipio in municipiosBanco:
            municipios.append({'idmunicipio':municipio["idmunicipio"], 'nome':municipio["nomemunicipio"], 'uf':municipio["uf"]})

        # Carrega os setores e subsetores do processo
        cur.execute(sqlsetores, processoelastic["idprocesso"])
        setoresBanco = cur.fetchall()
        setores = []
        for setor in setoresBanco:
            setores.append({'idsetor':setor["idsetor"],
                                    'codsetor':setor["codsetor"],
                                    'descsetor':setor["descsetor"],
                                    'codsubsetor': setor["codsubsetor"],
                                    'descsubsetor': setor["descsubsetor"]})

        # Carrega os arquivos do processo
        cur.execute(sqlarquivos, processoelastic["idprocesso"])
        arquivosBanco = cur.fetchall()
        arquivos = []
        for arquivo in arquivosBanco:
            arquivos.append({'idarquivo': arquivo["idarquivo"],
                                'numdoc': arquivo["numdoc"],
                                'coordenacao': arquivo["coordenacao"],
                                'situacao': arquivo["situacao"],
                                'link': arquivo["link"],
                                'nomearquivo': arquivo["nomearquivo"],
                                'textoarquivo': arquivo["textoarquivo"]})
            # print(str(arquivo["link"]))

        # Carrega os solicitantes do processo
        cur.execute(sqlsolicitantes, processoelastic["idprocesso"])
        solicitantesBanco = cur.fetchall()
        solicitantes = []
        for solicitante in solicitantesBanco:
            solicitantes.append({'idsolicitante': solicitante["idsolicitante"],
                                 'solicitante': solicitante["solicitante"],
                                 'cnpj': solicitante["cnpj"]})


        # Carrega as empresas do processo
        cur.execute(sqlempresas, processoelastic["idprocesso"])
        empresasBanco = cur.fetchall()
        empresas = []
        for empresa in empresasBanco:
            empresas.append({'idempresa': empresa["idempresa"],
                                 'nome': empresa["nome"]})


        processoelastic["abrangestados"] = estados
        processoelastic["abrangmunicipios"] = municipios
        processoelastic["setores"] = setores
        processoelastic["arquivos"] = arquivos
        processoelastic["solicitantes"] = solicitantes
        processoelastic["empresas"] = empresas

        print(str(processoelastic["dtvigenciafim"]))

        res = es.index(index="promocoes", doc_type='processos_seae', body=processoelastic, id=processoelastic["idprocesso"])

        sqlatualizarElastic = 'Update seae_processos p set p.atualizarElastic = 0 where p.idprocesso = (%s);'
        cur.execute(sqlatualizarElastic, processoelastic["idprocesso"])
        conn.commit()

    # print(res['created'])