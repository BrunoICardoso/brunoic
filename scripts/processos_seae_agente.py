import datetime
import logging
import re
from time import sleep
import time

import PyPDF2
import banco
import lxml.html
from safe_requests import get_request_and_stringize
from safe_requests import post_request_and_stringize
from safe_requests import safe_retrieve
from urllib.parse import quote
MAXDIAS = 730
MAXPREMIOS = 5000

path = '/home/zeeng-admin/zagentes/seae_pdfs/'
conn, cur = banco.conecta_banco()
seae_url = 'http://www1.seae.fazenda.gov.br/LITTERA/BuscaProcessos.aspx'
proc_url = 'http://www1.seae.fazenda.gov.br/LITTERA/DetalheProcessos.aspx?id='
file_path = 'http://www1.seae.fazenda.gov.br/LITTERA/'

payload1 = {
    "__EVENTARGUMENT": "",
    "__EVENTTARGET": "ctl00$ContentPlaceHolder1$btnLink",
    "__EVENTVALIDATION": "/wEWOALr69WeDwLogrfZCQLd6tHjCwKklvzcCwK7vMaaCQL18dHtCwLlnvuDBwLinvuDBwL+nvuDBwL/nvuDBwL8nvuDBwLhlt3aAQLx+fe0DQLu+fe0DQLv+fe0DQLs+fe0DQLt+fe0DQLq+fe0DQLr+fe0DQLo+fe0DQL5+fe0DQL2+fe0DQLu+be3DQLu+bu3DQLu+b+3DQLu+YO3DQLu+Ye3DQLu+Yu3DQLu+Y+3DQLu+ZO3DQLu+de0DQLu+du0DQLv+be3DQLv+bu3DQLv+b+3DQLv+YO3DQLv+Ye3DQLv+Yu3DQLv+Y+3DQLv+ZO3DQLv+de0DQLv+du0DQL2+du0DQLCrJb6AwL7veumAQLCx5e7CAKk74OVCAKk7/+VCAKk7+uVCAKk7+eVCAKk7/OVCAKk7++VCAKk79uVCAKk79eVCALK5bOlCQKhzJHYDFKVnKZ5h64IGJh1TIL6KqJ6J2e0",
    "__LASTFOCUS": "",
    "__VIEWSTATE": "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",
    "__VIEWSTATEGENERATOR": "E44C1DD4",
    "ctl00$ContentPlaceHolder1$cbxSetor": 0,
    "ctl00$ContentPlaceHolder1$cbxTipo": 0,
    "ctl00$ContentPlaceHolder1$csPesq": "rdbE",
    "ctl00$ContentPlaceHolder1$dsc_num_processo": "",
    "ctl00$ContentPlaceHolder1$nom_interessado": ""
}

payload2 = {
    "__EVENTARGUMENT": str,
    "__EVENTTARGET": "ctl00$ContentPlaceHolder1$gvProcessos",
    "__EVENTVALIDATION": "/wEWOALr69WeDwLogrfZCQLd6tHjCwKklvzcCwK7vMaaCQL18dHtCwLlnvuDBwLinvuDBwL+nvuDBwL/nvuDBwL8nvuDBwLhlt3aAQLx+fe0DQLu+fe0DQLv+fe0DQLs+fe0DQLt+fe0DQLq+fe0DQLr+fe0DQLo+fe0DQL5+fe0DQL2+fe0DQLu+be3DQLu+bu3DQLu+b+3DQLu+YO3DQLu+Ye3DQLu+Yu3DQLu+Y+3DQLu+ZO3DQLu+de0DQLu+du0DQLv+be3DQLv+bu3DQLv+b+3DQLv+YO3DQLv+Ye3DQLv+Yu3DQLv+Y+3DQLv+ZO3DQLv+de0DQLv+du0DQL2+du0DQLCrJb6AwL7veumAQLCx5e7CAKk74OVCAKk7/+VCAKk7+uVCAKk7+eVCAKk7/OVCAKk7++VCAKk79uVCAKk79eVCALK5bOlCQKhzJHYDFKVnKZ5h64IGJh1TIL6KqJ6J2e0",
    "__LASTFOCUS": "",
    "__VIEWSTATE": "/wEPDwULLTIwMjMzMjcwMDYPZBYCZg9kFgICAw9kFgICAQ9kFgYCAw9kFgQCAg9kFgRmD2QWAgICDxAPFgYeDURhdGFUZXh0RmllbGQFCERzY19UaXBvHg5EYXRhVmFsdWVGaWVsZAUGaWRUaXBvHgtfIURhdGFCb3VuZGdkEBUFACpQcm9jZXNzbyBkZSBBbsOhbGlzZSBkZSBJbnRlcmVzc2UgUMO6YmxpY28XUHJvY2Vzc28gQWRtaW5pc3RyYXRpdm8WUHJvbW/Dp8O1ZXMgQ29tZXJjaWFpcxRSZWR1w6fDo28gVGFyaWbDoXJpYRUFATABOQE1ATYBNxQrAwVnZ2dnZxYBZmQCAQ9kFgICAg8QDxYGHwAFCU5vbV9TZXRvch8BBQdpZFNldG9yHwJnZBAVHwAWMDEuIEVYVFJBw4fDg08gTUlORVJBTA8wMi4gQUdSSUNVTFRVUkEhMDMuIFBFQ1XDgVJJQSBFIFBST0RVw4fDg08gQU5JTUFMGDA0LiBJTkTDmlNUUklBIE1BREVSRUlSQRkwNS4gSU5Ew5pTVFJJQSBERSBNw5NWRUlTIjA2LiBJTkTDmlNUUklBIERFIFBBUEVMIEUgQ0VMVUxPU0UbMDcuIElORMOaU1RSSUEgQUxJTUVOVMONQ0lBGTA4LiBJTkTDmlNUUklBIERFIEJFQklEQVMIMDkuIEZVTU8tMTAuIElORMOaU1RSSUEgVMOKWFRJTCBFIERFIFBST0RVVE9TIERFIENPVVJPIjExLiBDT01VTklDQcOHw4NPIEUgRU5UUkVURU5JTUVOVE8nMTIuIElORMOaU1RSSUEgUVXDjU1JQ0EgRSBQRVRST1FVw41NSUNBKDEzLiBJTkTDmlNUUklBIERFIFBMw4FTVElDT1MgRSBCT1JSQUNIQVM1MTQuIElORMOaU1RSSUEgRkFSTUFDw4pVVElDQSBFIERFIFBST0RVVE9TIERFIEhJR0lFTkU2MTUuIElORMOaU1RSSUEgREUgUFJPRFVUT1MgREUgTUlORVJBSVMgTsODTy1NRVTDgUxJQ09TGzE2LiBJTkTDmlNUUklBIE1FVEFMw5pSR0lDQRgxNy4gSU5Ew5pTVFJJQSBNRUPDgk5JQ0EdMTguIElORMOaU1RSSUEgTUVDw4JOSUNBIExFVkUgMTkuIElORMOaU1RSSUEgRUxFVFJPRUxFVFLDlE5JQ0EzMjAuIElORMOaU1RSSUEgREUgSU5GT1JNw4FUSUNBIEUgVEVMRUNPTVVOSUNBw4fDlUVTLjIxLiBJTkTDmlNUUklBIEFVVE9NT0JJTElTVElDQSBFIERFIFRSQU5TUE9SVEUWMjIuIENPTlNUUlXDh8ODTyBDSVZJTBgyMy4gQ09Nw4lSQ0lPIEFUQUNBRElTVEEXMjQuIENPTcOJUkNJTyBWQVJFSklTVEEpMjUuIFNFUlZJw4dPUyBERSBUUkFOU1BPUlRFIEUgQVJNQVpFTkFHRU0tMjYuIFNFUlZJw4dPUyBFU1NFTkNJQUlTIEUgREUgSU5GUkEtRVNUUlVUVVJBFDI3LiBTRVJWScOHT1MgR0VSQUlTGTI4LiBTRVJWScOHT1MgRklOQU5DRUlST1MaMjkuIFNFR1VST1MgRSBQUkVWSUTDik5DSUESOTkuIE7Do28gaW5mb3JtYWRvFR8BMAExATIBMwE0ATUBNgE3ATgBOQIxMAIxMQIxMgIxMwIxNAIxNQIxNgIxNwIxOAIxOQIyMAIyMQIyMgIyMwIyNAIyNQIyNgIyNwIyOAIyOQI5ORQrAx9nZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnFgFmZAIDD2QWBGYPZBYCAgIPDxYCHgRUZXh0ZWRkAgEPZBYCAgIPEA8WAh8CZ2RkFgBkAgsPDxYCHwMFKzxiPlRvdGFsIGRlIFJlZ2lzdHJvcyBlbmNvbnRyYWRvczo8L2I+IDYyODdkZAINDzwrAA0CAA8WBB8CZx4LXyFJdGVtQ291bnQCjzFkARAWA2YCAwIEFgM8KwAFAQAWAh4KSGVhZGVyVGV4dAUOTW92aW1lbnRhw6fDo288KwAFAQAWAh8FBQpTaXR1YcOnw6NvPCsABQEAFgIfBQUITsK6IERvY3MWA2ZmZhYCZg9kFiACAQ9kFgpmDw8WAh8DBQoxOS8wMS8yMDE3ZGQCAQ9kFgJmDxUCETE4MTAxMDAwMDM4MjAxNzY2FDE4MTAxLjAwMDAzOC8yMDE3LTY2ZAICDw8WAh8DBWFDb29wZXJhdGl2YSBkZSBDcmVkaXRvIGUgSW52ZXN0aW1lbnRvIGRlIExpdnJlIEFkbWlzc2FvIEFncm9lbXByZXNhcmlhbCAtIFNpY3JlZGkgQWdyb2VtcHJlc2FyaWFsZGQCAw8PFgIfAwUPRW0gYW4mIzIyNTtsaXNlZGQCBA8PFgIfAwUBMGRkAgIPZBYKZg8PFgIfAwUKMTgvMDEvMjAxN2RkAgEPZBYCZg8VAhExODEwMTAwMDAzMzIwMTczMxQxODEwMS4wMDAwMzMvMjAxNy0zM2QCAg8PFgIfAwWcAUMmIzIyNjttYXJhIGRlIERpcmlnZW50ZXMgTG9qaXN0YXMgZGUgU2VhcmEgZSBDb29wZXJhdGl2YSBkZSBDciYjMjMzO2RpdG8gZGUgTGl2cmUgQWRtaXNzJiMyMjc7byBkZSBBc3NvY2lhZG9zIGRvIEFsdG8gVXJ1Z3VhaSBDYXRhcmluZW5zZSAtIFNpY29vYiBDcmVkaWF1Y2RkAgMPDxYCHwMFD0VtIGFuJiMyMjU7bGlzZWRkAgQPDxYCHwMFATBkZAIDD2QWCmYPDxYCHwMFCjExLzAxLzIwMTdkZAIBD2QWAmYPFQIRMTgxMDEwMDAwMjgyMDE3MjEUMTgxMDEuMDAwMDI4LzIwMTctMjFkAgIPDxYCHwMFTUNvb3BlcmF0aXZhIGRlIENyJiMyMzM7ZGl0byBkZSBMaXZyZSBBZG1pc3MmIzIyNztvIGRlIEFzc29jaWFkb3MgQ2VudHJvIFNlcnJhZGQCAw8PFgIfAwUPRW0gYW4mIzIyNTtsaXNlZGQCBA8PFgIfAwUBMGRkAgQPZBYKZg8PFgIfAwUKMTEvMDEvMjAxN2RkAgEPZBYCZg8VAhExODEwMTAwMDAyNjIwMTczMRQxODEwMS4wMDAwMjYvMjAxNy0zMWQCAg8PFgIfAwVNQ29vcGVyYXRpdmEgZGUgQ3ImIzIzMztkaXRvIGRlIExpdnJlIEFkbWlzcyYjMjI3O28gZGUgQXNzb2NpYWRvcyBDZW50cm8gU2VycmFkZAIDDw8WAh8DBQ9FbSBhbiYjMjI1O2xpc2VkZAIEDw8WAh8DBQEwZGQCBQ9kFgpmDw8WAh8DBQoxMS8wMS8yMDE3ZGQCAQ9kFgJmDxUCETE4MTAxMDAwMDI3MjAxNzg2FDE4MTAxLjAwMDAyNy8yMDE3LTg2ZAICDw8WAh8DBU1Db29wZXJhdGl2YSBkZSBDciYjMjMzO2RpdG8gZGUgTGl2cmUgQWRtaXNzJiMyMjc7byBkZSBBc3NvY2lhZG9zIENlbnRybyBTZXJyYWRkAgMPDxYCHwMFD0VtIGFuJiMyMjU7bGlzZWRkAgQPDxYCHwMFATBkZAIGD2QWCmYPDxYCHwMFCjEwLzAxLzIwMTdkZAIBD2QWAmYPFQIRMTgxMDEwMDAwMTIyMDE3MTgUMTgxMDEuMDAwMDEyLzIwMTctMThkAgIPDxYCHwMFJFZpc2EgZG8gQnJhc2lsIEVtcHJlZW5kaW1lbnRvcyBMdGRhLmRkAgMPDxYCHwMFD0VtIGFuJiMyMjU7bGlzZWRkAgQPDxYCHwMFATBkZAIHD2QWCmYPDxYCHwMFCjEwLzAxLzIwMTdkZAIBD2QWAmYPFQIRMTgxMDEwMDAwMTMyMDE3NjIUMTgxMDEuMDAwMDEzLzIwMTctNjJkAgIPDxYCHwMFM0ZvcnRCcmFzaWwgQWRtaW5pc3RyYWRvcmEgZGUgQ2FydG9lcyBkZSBDcmVkaXRvIFMvQWRkAgMPDxYCHwMFD0VtIGFuJiMyMjU7bGlzZWRkAgQPDxYCHwMFATBkZAIID2QWCmYPDxYCHwMFCjA1LzAxLzIwMTdkZAIBD2QWAmYPFQIRMTgxMDEwMDAwMDcyMDE3MTMUMTgxMDEuMDAwMDA3LzIwMTctMTNkAgIPDxYCHwMFiwFMVEQgQWRtaW5pc3RyYSYjMjMxOyYjMjI3O28gZSBQYXJ0aWNpcGEmIzIzMTsmIzI0NTtlcyBTL0EsIE1hZ2F6aW5lIEx1aXphIFMvQSBlIEx1aXphIENyZWQgUy9BIFNvYy4gZGUgQ3ImIzIzMztkaXRvLCBGaW5hbmMuIGUgSW52ZXN0aW1lbnRvZGQCAw8PFgIfAwUPRW0gYW4mIzIyNTtsaXNlZGQCBA8PFgIfAwUBMGRkAgkPZBYKZg8PFgIfAwUKMzAvMTIvMjAxNmRkAgEPZBYCZg8VAhExODEwMTAwMDgwODIwMTY5MBQxODEwMS4wMDA4MDgvMjAxNi05MGQCAg8PFgIfAwUQSWRlYWwgSW52ZXN0IFMvQWRkAgMPDxYCHwMFD0VtIGFuJiMyMjU7bGlzZWRkAgQPDxYCHwMFATBkZAIKD2QWCmYPDxYCHwMFCjI5LzEyLzIwMTZkZAIBD2QWAmYPFQIRMTgxMDEwMDA4MDYyMDE2MDkUMTgxMDEuMDAwODA2LzIwMTYtMDlkAgIPDxYCHwMFNUNvb3BlcmF0aXZhIGRlIENyZWRpdG8gZGUgTGl2cmUgQWRtaXNzYW8gUGFyYW5hcGFuZW1hZGQCAw8PFgIfAwUPRW0gYW4mIzIyNTtsaXNlZGQCBA8PFgIfAwUBMGRkAgsPZBYKZg8PFgIfAwUKMjkvMTIvMjAxNmRkAgEPZBYCZg8VAhExODEwMTAwMDgwNzIwMTY0NRQxODEwMS4wMDA4MDcvMjAxNi00NWQCAg8PFgIfAwU1Q29vcGVyYXRpdmEgZGUgQ3JlZGl0byBkZSBMaXZyZSBBZG1pc3NhbyBQYXJhbmFwYW5lbWFkZAIDDw8WAh8DBQ9FbSBhbiYjMjI1O2xpc2VkZAIEDw8WAh8DBQEwZGQCDA9kFgpmDw8WAh8DBQoyNy8xMi8yMDE2ZGQCAQ9kFgJmDxUCETE4MTAxMDAwODA1MjAxNjU2FDE4MTAxLjAwMDgwNS8yMDE2LTU2ZAICDw8WAh8DBWhKIENydXogTHRkYS4gKEZhcm1hY2lhIGRvIENvbnN1bWlkb3IpLiBlIEJyYXNpbCBDYXJkIEFkbWluaXN0cmFkb3JhIGRlIENhcnQmIzIyNztvIGRlIENyJiMyMzM7ZGl0byBMdGRhLmRkAgMPDxYCHwMFD0VtIGFuJiMyMjU7bGlzZWRkAgQPDxYCHwMFATBkZAIND2QWCmYPDxYCHwMFCjIzLzEyLzIwMTZkZAIBD2QWAmYPFQIRMTgxMDEwMDA4MDQyMDE2MTAUMTgxMDEuMDAwODA0LzIwMTYtMTBkAgIPDxYCHwMFbUNvb3BlcmF0aXZhIGRlIENyJiMyMzM7ZGl0byBkZSBMaXZyZSBBZG1pc3MmIzIyNztvIFVuaSYjMjI3O28gZG8gQ2VudHJvIE9lc3RlIGRlIE1pbmFzIEx0ZGEuIC0gU2ljb29iIENyZWRlc3BkZAIDDw8WAh8DBQ9FbSBhbiYjMjI1O2xpc2VkZAIEDw8WAh8DBQEwZGQCDg9kFgpmDw8WAh8DBQoyMy8xMi8yMDE2ZGQCAQ9kFgJmDxUCETE4MTAxMDAwODAzMjAxNjY3FDE4MTAxLjAwMDgwMy8yMDE2LTY3ZAICDw8WAh8DBURDaXR5c3BhY2UgRW1wcmVlbmRpbWVudG9zIEVpcmVsaSBlIEFwbHViIENhcGl0YWxpemEmIzIzMTsmIzIyNztvIFMvQWRkAgMPDxYCHwMFD0VtIGFuJiMyMjU7bGlzZWRkAgQPDxYCHwMFATBkZAIPD2QWCmYPDxYCHwMFCjIzLzEyLzIwMTZkZAIBD2QWAmYPFQIRMTgxMDEwMDA4MDIyMDE2MTIUMTgxMDEuMDAwODAyLzIwMTYtMTJkAgIPDxYCHwMFeUNvb3BlcmF0aXZhIGRlIENyJiMyMzM7ZGl0byBlIEludmVzdGltZW50byBkZSBMaXZyZSBBZG1pc3MmIzIyNztvIGRvIENlbnRybyBTdWwgZG8gUGFyYW4mIzIyNTsgLSBTaWNyZWRpIENlbnRybyBTdWwgUFIvU0NkZAIDDw8WAh8DBQ9FbSBhbiYjMjI1O2xpc2VkZAIEDw8WAh8DBQEwZGQCEA8PFgIeB1Zpc2libGVoZGQYAgUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgMFHmN0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkcmRiRQUfY3RsMDAkQ29udGVudFBsYWNlSG9sZGVyMSRyZGJPVQUfY3RsMDAkQ29udGVudFBsYWNlSG9sZGVyMSRyZGJPVQUlY3RsMDAkQ29udGVudFBsYWNlSG9sZGVyMSRndlByb2Nlc3Nvcw88KwAKAQgCpANkGgQdpNxKN89Mx0tysFIq0v8Ii0Q=",
    "__VIEWSTATEGENERATOR": "E44C1DD4",
    "ctl00$ContentPlaceHolder1$cbxSetor": 0,
    "ctl00$ContentPlaceHolder1$cbxTipo": 0,
    "ctl00$ContentPlaceHolder1$csPesq": "rdbE",
    "ctl00$ContentPlaceHolder1$dsc_num_processo": "",
    "ctl00$ContentPlaceHolder1$nom_interessado": ""
}

sql_insert_proc = 'INSERT INTO seae_processos (`dtprocesso`, `numprocesso`, `interessados`, `numdocs`) ' \
                  'VALUES (%s, %s, %s, %s)'

sql_check_situ = 'SELECT * FROM seae_situacoes WHERE `descricao` = (%s)'

sql_insert_new_situ = 'INSERT INTO seae_situacoes (`descricao`) VALUES (%s)'

sql_insert_proc_situ = 'INSERT INTO seae_mov_situacao (`idprocesso`, `idsituacao`, `dtsituacao`) VALUES (%s, %s, %s)'

sql_check_setor = 'SELECT `idsetor` FROM seae_setores_proc WHERE codsetor = (%s)'

sql_insert_setor = 'INSERT INTO seae_setores_proc (`codsetor`, `descricao`) VALUES (%s, %s)'

sql_check_subsetor = 'SELECT `idsubsetor` FROM seae_subsetores_proc WHERE codsubsetor = (%s) AND idsetor = (%s)'

sql_insert_subsetor = 'INSERT INTO seae_subsetores_proc (`idsetor`, `codsubsetor`, `descricao`) VALUES (%s, %s, %s)'

sql_insert_proc_setor = 'INSERT INTO seae_processo_setor (`idprocesso`, `idsetor`, `idsubsetor`) VALUES (%s, %s, %s)'

sql_insert_arquivo = 'INSERT INTO seae_arquivos_proc (`idprocesso`, `numdoc`, `coordenacao`, ' \
                     '`situacao`, `link`, `nomearquivo`, `textoarquivo`) VALUES (%s, %s, %s, %s, %s, %s, %s)'

sql_check_proc = 'SELECT * FROM seae_processos WHERE `numprocesso` = (%s)'

sql_check_ultima_situ = 'SELECT `idsituacao` FROM seae_mov_situacao WHERE `idprocesso` = (%s) ' \
                        'ORDER BY `dtsituacao` DESC LIMIT 1'

sql_check_file = 'SELECT `idarquivo` FROM seae_arquivos_proc WHERE `numdoc` = (%s) AND `idprocesso` = (%s)'

sql_insert_solic = 'INSERT INTO seae_processo_solicitantes (`idprocesso`, `solicitante`, `cnpj`) VALUES (%s, %s, %s)'

sql_set_dados = 'UPDATE seae_processos SET `dtvigenciaini` = (%s), `dtvigenciafim` = (%s), `premios` = (%s), ' \
                '`valortotalpremios` = (%s), `modalidade` = (%s), `formacontemplacao` = (%s), `abrangencia_nacional` = (%s), ' \
                '`atualizarElastic` = 1, `dadosExtraidos` = 1 WHERE `idprocesso` = (%s)'

sql_set_atualizarelastic = 'UPDATE seae_processos SET `atualizarElastic` = 1 WHERE `idprocesso` = (%s)'

sql_update_numdocs = 'UPDATE seae_processos SET `numdocs` = (%s) WHERE `idprocesso` = (%s)'


def get_idsituacao(situacao):
    cur.execute(sql_check_situ, situacao)
    result = cur.fetchone()
    if result:
        logging.info('Situação %s já existe. Retornando id.', situacao)
        idsituacao = result['idsituacao']
    else:
        try:
            logging.info('Situação %s ainda não existe. Inserindo-a no banco.', situacao)
            cur.execute(sql_insert_new_situ, situacao)
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID()")
            idsituacao = cur.fetchone()['LAST_INSERT_ID()']
        except Exception as excpt:
            logging.critical('%s', excpt)
            raise Exception
    
    return idsituacao

 
def get_insert_setores(idprocesso, setores):
    arrsetores = lxml.html.tostring(setores, encoding='utf8').decode('utf8').split('<br>')
    for s in arrsetores[1:-1]:
        setor = s.split('\\')[0]
        codsetor = int(re.findall('\d+', setor)[0])
        descrsetor = re.findall('\D+', setor)[0].replace('. ', "")
        subsetor = s.split('\\')[1]
        codsubsetor = int(re.findall('\d+', subsetor)[0])
        descrsubsetor = re.findall('\D+', subsetor)[0].replace('. ', "")

        cur.execute(sql_check_setor, codsetor)
        result = cur.fetchone()
        if result:
            idsetor = result['idsetor']
            logging.info('Setor de código %d já existe.', codsetor)
        else:
            logging.info('Setor de código %d não existe ainda. Adicionando-o.', codsetor)
            cur.execute(sql_insert_setor, (codsetor, descrsetor))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID()")
            idsetor = cur.fetchone()['LAST_INSERT_ID()']

        cur.execute(sql_check_subsetor, (codsubsetor, idsetor))
        result = cur.fetchone()
        if result:
            idsubsetor = result['idsubsetor']
            logging.info('Subsetor de código %d do setor de código %d já existe.', codsubsetor, codsetor)
        else:
            logging.info('Subsetor de código %d do setor de código %d não existe ainda. Adicionando-o.',
                         codsubsetor, codsetor)
            cur.execute(sql_insert_subsetor, (idsetor, codsubsetor, descrsubsetor))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID()")
            idsubsetor = cur.fetchone()['LAST_INSERT_ID()']

        cur.execute(sql_insert_proc_setor, (idprocesso, idsetor, idsubsetor))
        conn.commit()


def insert_arquivos(idprocesso, linhas):
    logging.info('Iniciando a captura de arquivos do processo %d', idprocesso)
    for linha in linhas[1:]:
        pdfinfo = linha.cssselect('td')
        numdoc = pdfinfo[0].text_content()
        cur.execute(sql_check_file, (idprocesso, numdoc))
        resultado = cur.fetchone()
        if resultado:
            logging.info('Arquivo %s já cadastrado.', numdoc)
            continue
        coordenacao = pdfinfo[1].text_content()
        situacao = pdfinfo[2].text_content()
        link = file_path + quote(pdfinfo[0].cssselect('a')[0].attrib['href'])
        nomearquivo = link[link.rfind('/') + 1:]
        safe_retrieve(link, path + nomearquivo)
        pdffile = open(path + nomearquivo, 'rb')
        pdfreader = PyPDF2.PdfFileReader(pdffile)
        numpags = pdfreader.numPages
        texto = ''
        for pag in range(numpags):
            texto = texto + pdfreader.getPage(pag).extractText()
            # texto = texto.replace('\n', "")
        try:
            cur.execute(sql_insert_arquivo,
                        (idprocesso, numdoc, coordenacao, situacao, link, nomearquivo, texto))
            conn.commit()
            logging.info('Arquivo %s baixado e cadastrado no banco', numdoc)
        except Exception as excpt:
            logging.critical('%s', excpt)
            exit()


def get_internal_info(idprocesso, numprocesso, numdocs):
    numproctratado = numprocesso.replace("/", "").replace(".", "").replace("-", "")
    resposta = get_request_and_stringize(proc_url + numproctratado)
    tree = lxml.html.fromstring(resposta)
    itens = tree.cssselect('div.item')
    setores = itens[2].cssselect('span#lbSetor')[0]
    get_insert_setores(idprocesso, setores)
    if int(numdocs) > 0:
        linhas = tree.cssselect('tr')
        insert_arquivos(idprocesso, linhas)
    else:
        logging.info('Nenhum arquivo do processo de id %d a baixar.', idprocesso)
    sleep(60)


def insert_processo(colunas):
    dtprocesso = datetime.datetime.strptime(colunas[0].text_content(), "%d/%m/%Y").date()
    hoje = datetime.date.today()
    if (hoje - dtprocesso).days >= 730:
        logging.info('Processo de 2 anos encontrado. Encerrando agente.')
        exit()
    numprocesso = colunas[1].text_content()
    interessados = colunas[2].text_content()
    numdocs = colunas[4].text_content()

    try:
        cur.execute(sql_insert_proc, (dtprocesso, numprocesso, interessados, numdocs))
        conn.commit()
        logging.info('Processo %s inserido.', numprocesso)
    except Exception as excpt:
        logging.critical('%s', excpt)
        exit()

    cur.execute("SELECT LAST_INSERT_ID()")
    idprocesso = cur.fetchone()['LAST_INSERT_ID()']

    situacao = colunas[3].text_content()
    idsituacao = get_idsituacao(situacao)
    dtsituacao = datetime.date.today()
    try:
        cur.execute(sql_insert_proc_situ, (idprocesso, idsituacao, dtsituacao))
        conn.commit()
        logging.info('Situação %d do novo processo %d inserida', idsituacao, idprocesso)
    except Exception as excpt:
        logging.critical('%s', excpt)
        exit()

    get_internal_info(idprocesso, numprocesso, numdocs)


def update_processo(processo, colunas):
    situacao = colunas[3].text_content()
    idsituacao = get_idsituacao(situacao)
    cur.execute(sql_check_ultima_situ, processo['idprocesso'])
    resultado = cur.fetchone()
    if idsituacao != resultado['idsituacao']:
        dtsituacao = datetime.date.today()
        cur.execute(sql_insert_proc_situ, (processo['idprocesso'], idsituacao, dtsituacao))
        cur.execute(sql_set_atualizarelastic, processo['idprocesso'])
        conn.commit()
    numdocs = colunas[4].text_content()
    if int(numdocs) != processo['numdocs']:
        numproctratado = processo['numprocesso'].replace("/", "").replace(".", "").replace("-", "")
        resposta = get_request_and_stringize(proc_url + numproctratado)
        tree = lxml.html.fromstring(resposta)
        linhas = tree.cssselect('tr')
        insert_arquivos(processo['idprocesso'], linhas)
        cur.execute(sql_update_numdocs, (numdocs, processo['idprocesso']))
        cur.execute(sql_set_atualizarelastic, processo['idprocesso'])
        conn.commit()


def agente_promo():
    resposta = post_request_and_stringize(seae_url, data=payload1)
    tree = lxml.html.fromstring(resposta)
    linhas_tabela = tree.cssselect('div.tb > div > table > tr')
    pags = linhas_tabela[-1].cssselect('td')
    viewstate = tree.cssselect('input#__VIEWSTATE')[0].attrib['value']
    eventvalidation = tree.cssselect('input#__EVENTVALIDATION')[0].attrib['value']
    while True:
        for pag in pags[2:-1]:
            for linha in linhas_tabela[1:-1]:
                colunas = linha.cssselect('td')
                numprocesso = colunas[1].text_content()
                cur.execute(sql_check_proc, numprocesso)
                resultado = cur.fetchone()
                if not resultado:
                    logging.info('Processo %s não encontrado. Iniciando inserção.', numprocesso)
                    insert_processo(colunas)
                else:
                    hoje = datetime.date.today()
                    if (hoje - resultado['dtprocesso']).days >= 730:
                        logging.info('Processo de 2 anos encontrado. Encerrando agente.')
                        exit()
                    logging.info('Processo %s encontrado. Iniciando atualização.', numprocesso)
                    update_processo(resultado, colunas)

            pag_str = re.findall('Page\$\d+', pag.cssselect('a')[0].attrib['href'])[0]
            payload2["__EVENTARGUMENT"] = pag_str
            payload2["__VIEWSTATE"] = viewstate
            payload2["__EVENTVALIDATION"] = eventvalidation
            sleep(10)
            resposta = post_request_and_stringize(url=seae_url, data=payload2)
            tree = lxml.html.fromstring(resposta)
            viewstate = tree.cssselect('input#__VIEWSTATE')[0].attrib['value']
            eventvalidation = tree.cssselect('input#__EVENTVALIDATION')[0].attrib['value']
            linhas_tabela = tree.cssselect('div.tb > div > table > tr')
        try:
            pag_str = re.findall('Page\$\d+', pags[-1].cssselect('a')[0].attrib['href'])[0]
        except IndexError:
            logging.info('Última página capturada.')
            return
        payload2["__EVENTARGUMENT"] = pag_str
        payload2["__VIEWSTATE"] = viewstate
        payload2["__EVENTVALIDATION"] = eventvalidation
        resposta = post_request_and_stringize(url=seae_url, data=payload2)
        tree = lxml.html.fromstring(resposta)
        viewstate = tree.cssselect('input#__VIEWSTATE')[0].attrib['value']
        eventvalidation = tree.cssselect('input#__EVENTVALIDATION')[0].attrib['value']
        linhas_tabela = tree.cssselect('div.tb > div > table > tr')
        pags = linhas_tabela[-1].cssselect('td')[1:]


def extrair_dados():
    sql = 'SELECT seae_processos.idprocesso, textoarquivo ' \
          'FROM seae_processos INNER JOIN seae_arquivos_proc ' \
          'ON seae_processos.idprocesso = seae_arquivos_proc.idprocesso ' \
          'WHERE numdocs > 0 AND dadosExtraidos = 0 AND nomearquivo like "%Certificado_de_Autoriza%"'
    cur.execute(sql)
    results = cur.fetchall()
    for result in results:
        texto_newline = result['textoarquivo']
        texto = result['textoarquivo'].replace('\n', '')
        cnpjs = re.findall('\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}', texto)
        idprocesso = result['idprocesso']
        texto_lista = texto_newline.split('\n')
        texto_lista = [item.strip() for item in texto_lista]
        cnpj_str = 'N.º do CNPJ:'
        for index in range(len(cnpjs)):
            if len(cnpjs) == 1:
                sol_str = 'Solicitante:'
            else:
                sol_str = 'Solicitante' + ' ' + str(index + 1) + ':'
            indice = texto_lista.index(sol_str) + 1
            sol = texto_lista[indice] if texto_lista[indice + 1] == cnpj_str\
                else texto_lista[indice] + texto_lista[indice + 1]
            cnpj = cnpjs[index]
            cur.execute(sql_insert_solic, (idprocesso, sol, cnpj))

        vigencia = re.findall('\d{2}/\d{2}/\d{4} a \d{2}/\d{2}/\d{4}', texto)
        range_data = vigencia[0]
        dtinicial = datetime.datetime.strptime(range_data[:range_data.index(' ')], '%d/%m/%Y').date()
        dtfinal = datetime.datetime.strptime(range_data[range_data.rfind(' ') + 1:], '%d/%m/%Y').date()

        valor_total_str = 'Valor total dos prêmios:'
        local_str = 'Local de entrega dos prêmios:'
        indice = texto_lista.index(valor_total_str) + 1
        valor_total = ''
        while local_str not in texto_lista[indice]:
            valor_total += texto_lista[indice]
            indice += 1
        regex = re.compile('(?:\d{1,3}\.)*\d{1,3},\d{2}')
        valor_total = re.findall(regex, valor_total)[0]
        valor_total = valor_total.replace('.', '')
        valor_total = valor_total.replace(',', '.')

        premios_str = 'Quantidade e natureza dos prêmios:'
        indice = texto_lista.index(premios_str) + 1
        premios = ''
        while valor_total_str not in texto_lista[indice]:
            premios += texto_lista[indice] + '\n'
            indice += 1
        premios = premios[:MAXPREMIOS] if len(premios) > MAXPREMIOS else premios
        contemplacao_str = 'Forma de contemplação:'
        area_str = 'Área da promoção:'
        indice = texto_lista.index(contemplacao_str) + 1
        formacontemplacao = ''
        while area_str not in texto_lista[indice]:
            formacontemplacao += texto_lista[indice]
            indice += 1

        modalidade_str = 'Modalidade:'
        indice = texto_lista.index(modalidade_str) + 1
        modalidade = ''
        while contemplacao_str not in texto_lista[indice]:
            modalidade += texto_lista[indice]
            indice += 1

        periodo_str = 'Período de execução:'
        abrangencia_str = 'Área da promoção:'
        indice = texto_lista.index(abrangencia_str) + 1
        abrangencia = ''
        while periodo_str not in texto_lista[indice]:
            abrangencia += texto_lista[indice]
            indice += 1
        abrangencia_nacional = 1 if 'nacional' in abrangencia.lower() else 0
        try:
            cur.execute(sql_set_dados, (dtinicial, dtfinal, premios, valor_total, modalidade,
                                        formacontemplacao, abrangencia_nacional, idprocesso))
            conn.commit()
        except Exception as excpt:
            logging.critical('%s', excpt)
            exit()


if __name__ == "__main__":
    import sys
    start_time = time.time()
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    if sys.argv[1] == 'captura':
        agente_promo()
    elif sys.argv[1] == 'extrai':
        extrair_dados()
    else:
        logging.info('Comando: python3 ./procesos_seae_agente.py (captura|extrai)')
        exit()
    cur.close()
    conn.close()
    logging.info("Tempo de execução do script: %s segundos" % (time.time() - start_time))
    logging.info("Script executado com sucesso!")
