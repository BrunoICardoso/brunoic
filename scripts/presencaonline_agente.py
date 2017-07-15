import datetime
import logging
import time
import urllib.request

import banco
from safe_requests import get_request_and_stringize
from bs4 import BeautifulSoup

conn, cur = banco.conecta_banco()

alexa_url = 'http://www.alexa.com/siteinfo/'


def get_presenca_online():
    sql = 'SELECT `idempresa`,`urlsite` FROM empresas WHERE `excluido` = 0'
    cur.execute(sql)
    # results = cur.fetchall()

    results = "http://www.cocacola.com.br/pt/home/"

    for result in results:
        if result['urlsite'] is None:
            logging.info('Empresa não possui site.')
            continue
        else:
            sql_check_date = 'SELECT MAX(`datacaptura`) as datacaptura' \
                             ' FROM presenca_online_capturas WHERE `idempresa` =  (%s)'
            cur.execute(sql_check_date, result['idempresa'])
            resultado = cur.fetchone()
            if resultado['datacaptura'] is None:
                logging.info('Primeira captura de presença online da empresa %d', result['idempresa'])
                importa_alexa_v2(result['idempresa'], result['urlsite'])
            else:
                dataultimacaptura = resultado['datacaptura']
                datanow = datetime.date.today()
                if (datanow - dataultimacaptura.date()).days >= 7 or dataultimacaptura.month != datanow.month:
                    importa_alexa_v2(result['idempresa'], result['urlsite'])
                else:
                    logging.info("Presença online da empresa %d já foi capturada essa semana.", result['idempresa'])


def importa_alexa_v2(idempresa, urlsite):
    logging.info('Importando dados do Alexa para a empresa de id %d', idempresa)
    url = (alexa_url + urlsite)
    data = get_request_and_stringize(url)
    soup = BeautifulSoup(data, "lxml")

    sql = 'INSERT INTO presenca_online_capturas (`idempresa`, `datacaptura`, `rankglobal`, `rankbrasil`,' \
          '`visitaspesquisa`, `taxarejeicao`, `paginasvisitadas`, `tempovisita`, `audi_feminino`, `audi_masculino`,' \
          ' `audi_semfaculdade`, `audi_algumafaculdade`, `audi_posgraduacao`, `audi_faculdade`, `audi_casa`,' \
          ' `audi_escola`, `audi_trabalho`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    metricas_raw = []

    globalrank_raw = soup.find('span', attrs={'data-cat': 'globalRank'})
    if not globalrank_raw:
        logging.error('O site %s da empresa %d não existe.', urlsite, idempresa)
        return -1

    globalrank_raw = globalrank_raw.find('strong', attrs={'class': 'metrics-data'})
    metricas_raw.append(globalrank_raw.text)
    brasilrank_raw = soup.find('span', attrs={'data-cat': 'countryRank'})
    if brasilrank_raw:
        brasilrank_raw = brasilrank_raw.find('strong', attrs={'class': 'metrics-data'})
        metricas_raw.append(brasilrank_raw.text)
    else:
        metricas_raw.append(None)

    taxarejeicao_raw = soup.find('span', attrs={'data-cat': 'bounce_percent'})
    taxarejeicao_raw = taxarejeicao_raw.find('strong', attrs={'class': 'metrics-data'})
    metricas_raw.append(taxarejeicao_raw.text)

    paginasvisitadas_raw = soup.find('span', attrs={'data-cat': 'pageviews_per_visitor'})
    paginasvisitadas_raw = paginasvisitadas_raw.find('strong', attrs={'class': 'metrics-data'})
    metricas_raw.append(paginasvisitadas_raw.text)

    tempovisita_raw = soup.find('span', attrs={'data-cat': 'time_on_site'})
    tempovisita_raw = tempovisita_raw.find('strong', attrs={'class': 'metrics-data'})
    metricas_raw.append(tempovisita_raw.text)

    visitaspesquisa_raw = soup.find('span', attrs={'data-cat': 'search_percent'})
    visitaspesquisa_raw = visitaspesquisa_raw.find('strong', attrs={'class': 'metrics-data'})
    metricas_raw.append(visitaspesquisa_raw.text)

    metricas = []
    for texto in metricas_raw:
        if texto:
            for char in u' ,%\n':
                texto = texto.replace(char, '')
            metricas.append(texto)
        else:
            metricas.append(None)

    datacaptura = datetime.datetime.now()
    rankglobal = metricas[0] if metricas[0] != '-' else None
    rankbrasil = metricas[1] if metricas[1] != '-' else None
    taxarejeicao = metricas[2] if metricas[2] != '-' else None
    paginasvisitadas = metricas[3] if metricas[3] != '-' else None
    tempovisita = None
    try:
        tempovisita = datetime.datetime.strptime(metricas[4], '%M:%S').time() if metricas[4] != '-' else None
    except Exception:
        logging.error('Tempo de visita %s não é válido', metricas[4])

    visitaspesquisa = metricas[5] if metricas[5] != '-' else None

    genero = soup.find('div', attrs={'class': 'demo-gender'})
    generos = genero.find_all('div', attrs={'class': 'pybar-row'})

    gens = []
    for result in generos[1:-1]:
        gen = result.find_all('span', attrs={'class': 'pybar-bg'})
        gen1 = str(gen[0])
        gen1 = gen1[gen1.index(':') + 1:gen1.index('%')]
        gen2 = str(gen[1])
        gen2 = gen2[gen2.index(':') + 1:gen2.index('%')]
        audi_gen = (float(gen1) + float(gen2))/2

        gens.append(audi_gen)

    audi_masculino = gens[0]
    audi_feminino = gens[1]

    educacao = soup.find('div', attrs={'class': 'demo-education'})
    educacoes = educacao.find_all('div', attrs={'class': 'pybar-row'})

    edus = []
    for result in educacoes[1:-1]:
        edu = result.find_all('span', attrs={'class': 'pybar-bg'})
        edu1 = str(edu[0])
        edu1 = edu1[edu1.index(':') + 1:edu1.index('%')]
        edu2 = str(edu[1])
        edu2 = edu2[edu2.index(':') + 1:edu2.index('%')]
        audi_edu = (float(edu1) + float(edu2)) / 2

        edus.append(audi_edu)

    audi_semfaculdade = edus[0]
    audi_algumafaculdade = edus[1]
    audi_posgraduacao = edus[2]
    audi_faculdade = edus[3]

    local = soup.find('div', attrs={'class': 'demo-location'})
    locais = local.find_all('div', attrs={'class': 'pybar-row'})

    locs = []
    for result in locais[1:-1]:
        loc = result.find_all('span', attrs={'class': 'pybar-bg'})
        loc1 = str(loc[0])
        loc1 = loc1[loc1.index(':') + 1:loc1.index('%')]
        loc2 = str(loc[1])
        loc2 = loc2[loc2.index(':') + 1:loc2.index('%')]
        audi_loc = (float(loc1) + float(loc2)) / 2

        locs.append(audi_loc)

    audi_casa = locs[0]
    audi_escola = locs[1]
    audi_trabalho = locs[2]

    cur.execute(sql, (idempresa, datacaptura, rankglobal, rankbrasil, visitaspesquisa, taxarejeicao,
                      paginasvisitadas, tempovisita, audi_feminino, audi_masculino, audi_semfaculdade,
                      audi_algumafaculdade, audi_posgraduacao, audi_faculdade, audi_casa, audi_escola, audi_trabalho))
    conn.commit()

    sql = 'SELECT LAST_INSERT_ID()'
    cur.execute(sql)
    idcaptura = cur.fetchone()['LAST_INSERT_ID()']

    siteslink = soup.find('section', attrs={'id': 'linksin-panel-content'})
    corpolinks = siteslink.find('tbody')
    links = corpolinks.find_all('tr')

    sql = 'INSERT INTO presenca_online_linkadopor (`idcaptura`, `site`, `link`) VALUES (%s, %s, %s)'
    for result in links:
        link = result.find_all('td')
        if len(link) == 3:
            site = link[1].text
            urlcap = link[2].a['href']
            cur.execute(sql, (idcaptura, site, urlcap))
        else:
            break

    conn.commit()

    keywords = soup.find('section', attrs={'id': 'keywords-panel-content'})
    keywords = keywords.find('tbody')
    keywords = keywords.find_all('tr')

    sql = 'INSERT INTO presenca_online_palavraspesquisa (`idcaptura`, `palavra`, `percentual`) VALUES (%s, %s, %s)'
    for result in keywords:
        res = result.find_all('td')
        if len(res) == 2:
            palavra = res[0]['title']
            percentual = res[1].text[:-1]
            cur.execute(sql, (idcaptura, palavra, percentual))
        else:
            break

    conn.commit()


start_time = time.time()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
get_presenca_online()
logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
logging.info("Script executado com sucesso!")