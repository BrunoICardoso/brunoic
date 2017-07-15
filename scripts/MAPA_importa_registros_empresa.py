import banco
import pymysql
import datetime

conn, cur = banco.conecta_banco()

# Selecionar todos os CNPJs das empresas

sqlcnpjs = 'Select ' \
           'e.idempresa, cnpj ' \
           'From  ' \
           'cnpjempresa cnpj inner join ' \
           'empresas e on e.idempresa = cnpj.idempresa ' \
           'where  ' \
           'e.excluido = 0;'

sqlmapacaptura = 'SELECT ' \
		    '* ' \
            'FROM ' \
	            'mapa_dadoscaptura m ' \
            'WHERE  ' \
                'registro <> (%s) and ' \
	            'ltrim(rtrim(m.cnpj)) = (%s) ' \

sqlexisteregistro = 'Select * From mapa_registros where numregistro <> %s and numregistro = (%s)'

cur.execute(sqlcnpjs)
results = cur.fetchall()

print(len(results))
for empresa in results:

    print(empresa["cnpj"])
    # Pesquisar na tabela Mapa_dadosCaptura os CNPJs
    cur.execute(sqlmapacaptura, ('', empresa["cnpj"]))
    capturas = cur.fetchall()
    idempresa = empresa["idempresa"]
    for captura in capturas:
        print(captura["registro"])
        cur.execute(sqlexisteregistro, ('', captura["registro"]))
        registro= cur.fetchone()

        # Verificar se o registro encontrado já está na tabela MAPA_Registros
        if not registro:

            # Pegar o id das tabelas relacionadas aos campos.

            # Verifica se existe a estado
            sql = 'Select * from estados where uf = (%s)'
            cur.execute(sql, captura["uf"])
            estado = cur.fetchone()
            if (estado):
                idestado = estado["idestado"]

            #Verifica se existe a área
            idarea = None
            if captura["area"]:
                sql = 'Select * from mapa_areas where nome = (%s)'
                cur.execute(sql, captura["area"])
                aux = cur.fetchone()
                if(aux):
                    idarea = aux["idarea"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_areas(nome) values(%s)'
                    cur.execute(sql, captura["area"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idarea = cur.fetchone()["id"]

            # Verifica se existe a espécie
            idespecie = None
            if captura["especie"]:
                sql = 'Select * from mapa_especies where nome = (%s)'
                cur.execute(sql, captura["especie"])
                aux = cur.fetchone()
                if (aux):
                    idespecie = aux["idespecie"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_especies(nome) values(%s)'
                    cur.execute(sql, captura["especie"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idespecie = cur.fetchone()["id"]


            # Verifica se existe subespecie
            idsubespecie = None
            if captura["subespecie"]:
                sql = 'Select * from mapa_subespecie where nome = (%s)'
                cur.execute(sql, captura["subespecie"])
                aux = cur.fetchone()
                if (aux):
                    idsubespecie = aux["idsubespecie"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_subespecie(nome) values(%s)'
                    cur.execute(sql, captura["subespecie"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idsubespecie = cur.fetchone()["id"]


            # Verifica se existe base
            idbase = None
            if captura["base"]:
                sql = 'Select * from mapa_base where nome = (%s)'
                cur.execute(sql, captura["base"])
                aux = cur.fetchone()
                if (aux):
                    idbase = aux["idbase"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_base(nome) values(%s)'
                    cur.execute(sql, captura["base"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idbase = cur.fetchone()["id"]


            # Verifica se existe caracteristica
            idcaracteristica = None
            if captura["caracteristica"]:
                sql = 'Select * from mapa_caracteristica where nome = (%s)'
                cur.execute(sql, captura["caracteristica"])
                aux = cur.fetchone()
                if (aux):
                    idcaracteristica = aux["idcaracteristica"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_caracteristica(nome) values(%s)'
                    cur.execute(sql, captura["caracteristica"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idcaracteristica = cur.fetchone()["id"]

            # Verifica se existe atributo
            idatributo = None
            if captura["atributo"]:
                sql = 'Select * from mapa_atributo where nome = (%s)'
                cur.execute(sql, captura["atributo"])
                aux = cur.fetchone()
                if (aux):
                    idatributo = aux["idatributo"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_atributo(nome) values(%s)'
                    cur.execute(sql, captura["atributo"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idatributo = cur.fetchone()["id"]


            # Verifica se existe complemento
            idcomplemento = None
            if captura["complemento"]:
                sql = 'Select * from mapa_complemento where nome = (%s)'
                cur.execute(sql, captura["complemento"])
                aux = cur.fetchone()
                if (aux):
                    idcomplemento = aux["idcomplemento"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_complemento(nome) values(%s)'
                    cur.execute(sql, captura["complemento"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idcomplemento = cur.fetchone()["id"]


            # Verifica se existe origem
            print(captura["origem"])
            idorigem = None
            if captura["origem"]:
                sql = 'Select * from mapa_origens where nome = (%s)'
                cur.execute(sql, captura["origem"])
                aux = cur.fetchone()
                if (aux):
                    idorigem = aux["idorigem"]
                else:
                    # Se o campo não existe, insere e pega o novo ID
                    sql = 'Insert Into mapa_origens(nome) values(%s)'
                    cur.execute(sql, captura["origem"])
                    sql = 'SELECT LAST_INSERT_ID() as id;'
                    cur.execute(sql)
                    idorigem = cur.fetchone()["id"]


            # Verifica se existe origem
            idmarca = None
            if captura["marca"]:
                sql = 'select idmarca from marcas where nome = (%s) and idempresa = (%s)'
                cur.execute(sql, (captura["marca"], idempresa))
                aux = cur.fetchone()
                if (aux):
                    idmarca = aux["idmarca"]

            nomeMarca = captura["marca"]
            nomeProduto = captura["produto"]
            dataconcessao = datetime.datetime.strptime(captura["dataconcessao"],"%d/%m/%Y");
            numregistro = captura["registro"]
            status = ''



            print((captura["dataconcessao"]))
            if captura["status"] == '1':
                status = 'Registrado'

            modoaplicacao = ''

            print('idestado:' + str(idestado) + 'idarea:' + str(idarea) + 'idespecie :' + str(idespecie))
            print('idsubespecie:' +  str(idsubespecie) + 'idbase:' + str(idbase) + 'idcaracteristica:' + str(idcaracteristica))
            print('idatributo:' + str(idatributo) + 'idcomplemento:' + str(idcomplemento) + 'idorigem:' + str(idorigem))
            print('idempresa:' + str(idempresa) + 'idmarca:' + str(idmarca) + 'nomeMarca:' + str(nomeMarca) +'nomeProduto:'+ str(nomeProduto))
            print('dataconcessao: ' + str(dataconcessao.date()) + 'numregistro: '+ str(numregistro) + 'modoaplicacao:' + str(modoaplicacao))
            print('status:' + status + ' status: ' + captura["status"] )

            # Insere os dados na tabela MAPA_Registros
            sql = 'Insert Into mapa_registros(idestado, idarea, idespecie, idsubespecie, idbase, idcaracteristica, idatributo, idcomplemento, idorigem, idempresa, idmarca, nomeMarca, nomeProduto, dataconcessao, numregistro, modoaplicacao, status)' \
                    'values(%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            cur.execute(sql, (idestado, idarea, idespecie, idsubespecie, idbase, idcaracteristica, idatributo, idcomplemento, idorigem, idempresa, idmarca, nomeMarca, nomeProduto, dataconcessao, numregistro, modoaplicacao, status))

            conn.commit()

        else:
            print('Registro já cadastrado: ' + registro["numregistro"])