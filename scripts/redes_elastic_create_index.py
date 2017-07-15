from elasticsearch import Elasticsearch

es = Elasticsearch(['172.16.0.85:9200'])

face = {
    "mappings": {
        "fb_post": {
            "properties": {
                "idpost": {
                    "type": "integer"
                },
                "idperfil": {
                    "type": "integer"
                },
                "idempresa": {
                    "type": "integer"
                },
                "nomeempresa": {
                    "type": "string"
                },
                "postagem": {
                    "type": "string"
                },
                "compartilhamentos": {
                    "type": "integer"
                },
                "reacoes": {
                    "type": "integer"
                },
                "qtdcomentarios": {
                    "type": "integer"
                },
                "datahora": {
                    "type": "date",
                    "format": "dd/MM/yyy HH:mm:ss"
                },
                "nomeimagem": {
                    "type": "string"
                },
                "promocoes": {
                    "properties": {
                        "idpromocao": {
                            "type": "integer"
                        },
                        "nome": {
                            "type": "string"
                        }
                    }
                },
                "comentarios": {
                    "properties": {
                        "postagem": {
                            "type": "string"
                        },
                        "datahora": {
                            "type": "date",
                            "format": "dd/MM/yyy HH:mm:ss"
                        },
                        "curtidas": {
                            "type": "integer"
                        },
                        "nomeusuario": {
                            "type": "string"
                        },
                        "urlimagem": {
                            "type": "string"
                        },
                        "respostas": {
                            "properties": {
                                "postagem": {
                                    "type": "string"
                                },
                                "curtidas": {
                                    "type": "integer"
                                },
                                "urlimagem": {
                                    "type": "string"
                                },
                                "datahora": {
                                    "type": "date",
                                    "format": "dd/MM/yyy HH:mm:ss"
                                },
                                "nomeusuario": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

twitter = {
    "mappings": {
        "tw_post": {
            "properties": {
                "idpost": {
                    "type": "integer"
                },
                "idperfil": {
                    "type": "integer"
                },
                "idempresa": {
                    "type": "integer"
                },
                "nomeempresa": {
                    "type": "string"
                },
                "postagem": {
                    "type": "string"
                },
                "retweets": {
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
                "promocoes": {
                    "properties": {
                        "idpromocao": {
                            "type": "integer"
                        },
                        "nome": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    }
}

insta = {
    "mappings": {
        "insta_post": {
            "properties": {
                "idpost": {
                    "type": "integer"
                },
                "idperfil": {
                    "type": "integer"
                },
                "idempresa": {
                    "type": "integer"
                },
                "nomeempresa": {
                    "type": "string"
                },
                "postagem": {
                    "type": "string"
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
                "promocoes": {
                    "properties": {
                        "idpromocao": {
                            "type": "integer"
                        },
                        "nome": {
                            "type": "string"
                        }
                    }
                },
                "comentarios": {
                    "properties": {
                        "postagem": {
                            "type": "string"
                        },
                        "datahora": {
                            "type": "date",
                            "format": "dd/MM/yyy HH:mm:ss"
                        },
                        "nomeusuario": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    }
}

youtube = {
    "mappings": {
        "yt_video": {
            "properties": {
                "idvideo": {
                    "type": "integer"
                },
                "idcanal": {
                    "type": "integer"
                },
                "idempresa": {
                    "type": "integer"
                },
                "nomeempresa": {
                    "type": "string"
                },
                "descricao": {
                    "type": "string"
                },
                "visualizacoes": {
                    "type": "integer"
                },
                "curtidas": {
                    "type": "integer"
                },
                "descurtidas": {
                    "type": "integer"
                },
                "qtdcomentarios": {
                    "type": "integer"
                },
                "datahora": {
                    "type": "date",
                    "format": "dd/MM/yyy HH:mm:ss"
                },
                "nomeimagem": {
                    "type": "string"
                },
                "promocoes": {
                    "properties": {
                        "idpromocao": {
                            "type": "integer"
                        },
                        "nome": {
                            "type": "string"
                        }
                    }
                },
                "comentarios": {
                    "properties": {
                        "postagem": {
                            "type": "string"
                        },
                        "datahora": {
                            "type": "date",
                            "format": "dd/MM/yyy HH:mm:ss"
                        },
                        "curtidas": {
                            "type": "integer"
                        },
                        "nomeusuario": {
                            "type": "string"
                        },
                        "respostas": {
                            "properties": {
                                "postagem": {
                                    "type": "string"
                                },
                                "curtidas": {
                                    "type": "integer"
                                },
                                "datahora": {
                                    "type": "date",
                                    "format": "dd/MM/yyy HH:mm:ss"
                                },
                                "nomeusuario": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

indice = "dev"
es.indices.delete(index=indice + 'fb_posts')
es.indices.delete(index=indice + 'insta_posts')
es.indices.delete(index=indice + 'tw_posts')
es.indices.delete(index=indice + 'yt_videos')
es.indices.create(index=indice + 'fb_posts', body=face)
es.indices.create(index=indice + 'tw_posts', body=twitter)
es.indices.create(index=indice + 'insta_posts', body=insta)
es.indices.create(index=indice + 'yt_videos', body=youtube)
