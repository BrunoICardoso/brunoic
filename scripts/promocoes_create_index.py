from elasticsearch import Elasticsearch

es = Elasticsearch(['172.16.0.85:9200'])

mapping = {
    "mappings": {
        "promocao": {
            "properties": {
                "idpromocao": {
                    "type": "integer"
                },
                "nomepromocao": {
                    "type": "string"
                },
                "empresas": {
                    "properties": {
                        "idempresa": {
                            "type": "integer"
                        },
                        "nome": {
                            "type": "string"
                        }
                    }
                },
                "noticias": {
                    "properties": {
                        "idnoticia": {
                            "type": "integer"
                        },
                        "titulo": {
                            "type": "string"
                        },
                        "autor": {
                            "type": "string"
                        },
                        "datapublicacao": {
                            "type": "date",
                            "format": "yyyy-MM-dd"
                        },
                        "nomefonte": {
                            "type": "string"
                        },
                        "link": {
                            "type": "string"
                        },
                        "conteudo": {
                            "type": "string"
                        }
                    }
                },
                "postsfacebook": {
                    "properties": {
                        "idpost": {
                            "type": "integer"
                        },
                        "qtdcomentarios": {
                            "type": "integer"
                        },
                        "compartilhamentos": {
                            "type": "integer"
                        },
                        "datahora": {
                            "type": "date",
                            "format": "dd/MM/yyy HH:mm:ss"
                        },
                        "nomeimagem": {
                            "type": "string"
                        },
                        "postagem": {
                            "type": "string"
                        },
                        "curtidas": {
                            "type": "integer"
                        }
                    }
                },
                "poststwitter": {
                    "properties": {
                        "idpost": {
                            "type": "integer"
                        },
                        "curtidas": {
                            "type": "integer"
                        },
                        "retweets": {
                            "type": "integer"
                        },
                        "datahora": {
                            "type": "date",
                            "format": "dd/MM/yyy HH:mm:ss"
                        },
                        "nomeimagem": {
                            "type": "string"
                        },
                        "postagem": {
                            "type": "string"
                        }
                    }
                },
                "postsinstagram": {
                    "properties": {
                        "idpost": {
                            "type": "integer"
                        },
                        "qtdcomentarios": {
                            "type": "integer"
                        },
                        "curtidas": {
                            "type": "integer"
                        },
                        "datahora": {
                            "type": "date",
                            "format": "dd/MM/yyy HH:mm:ss"
                        },
                        "nomeimagem": {
                            "type": "string"
                        },
                        "postagem": {
                            "type": "string"
                        }
                    }
                },
                "videosyoutube": {
                    "properties": {
                        "idvideo": {
                            "type": "integer"
                        },
                        "qtdcomentarios": {
                            "type": "integer"
                        },
                        "curtidas": {
                            "type": "integer"
                        },
                        "descurtidas": {
                            "type": "integer"
                        },
                        "datahora": {
                            "type": "date",
                            "format": "dd/MM/yyy HH:mm:ss"
                        },
                        "nomeimagem": {
                            "type": "string"
                        },
                        "descricao": {
                            "type": "string"
                        },
                        "visualizacoes": {
                            "type": "integer"
                        }
                    }
                },
                "idmodalidade": {
                    "type": "integer"
                },
                "nomemodalidade": {
                    "type": "string",
                    "fielddata": "true",
                    "fields": {"raw": {"type": "string", "index": "not_analyzed"}}
                },
                "idorgaoregulador": {
                    "type": "integer"
                },
                "outrosinteressados": {
                    "type": "string"
                },
                "mecanicapromo": {
                    "type": "string"
                },
                "produtosparticipantes": {
                    "type": "string"
                },
                "nomeorgaoregulador": {
                    "type": "string"
                },
                "premiospromo": {
                    "type": "string"
                },
                "certificadoautorizacao": {
                    "type": "string"
                },
                "dtcadastro": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "dtvigenciaini": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "dtvigenciafim": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "valorpremios": {
                    "type": "double"
                },
                "linksitepromocao": {
                    "type": "string"
                },
                "linkfacebook": {
                    "type": "string"
                },
                "linkinstagram": {
                    "type": "string"
                },
                "linktwitter": {
                    "type": "string"
                },
                "linkyoutube": {
                    "type": "string"
                },
                "abrangencia_nacional": {
                    "type": "boolean"
                },
                "abrangestados": {
                    "properties": {
                        "idestado": {
                            "type": "integer"
                        },
                        "nome": {
                            "type": "string"
                        },
                        "uf": {
                            "type": "string"
                        }
                    }
                },
                "abrangmunicipios": {
                    "properties": {
                        "idmunicipio": {
                            "type": "integer"
                        },
                        "nome": {
                            "type": "string"
                        },
                        "uf": {
                            "type": "string"
                        },
                        "idestado": {
                            "type": "integer"
                        }
                    }
                },
                "linkregulamento": {
                    "type": "string"
                },
                "textoregulamento": {
                    "type": "string"
                },
                "arquivosregulamento": {
                    "properties": {
                        "nomearquivo": {
                            "type": "string"
                        },
                        "tipo": {
                            "type": "string"
                        }
                    }
                },
                "arquivosrelacionados": {
                    "properties": {
                        "nomearquivo": {
                            "type": "string"
                        },
                        "tipo": {
                            "type": "string"
                        },
                        "url": {
                            "type": "string"
                        }
                    }
                },
                "imagens": {
                    "properties": {
                        "linkimagem": {
                            "type": "string"
                        },
                        "nomeimagem": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    }
}

indice = "dev"
es.indices.delete(index=indice + 'promocoes')
es.indices.create(index=indice + 'promocoes', body=mapping)

# es.indices.put_mapping(index=indice + 'promocoes', body=mapping, doc_type='promocao')
