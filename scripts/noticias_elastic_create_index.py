from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['172.16.61.3:9200'])

mapping = {
    "mappings":{
    "noticias":{
      "properties":{
        "idnoticia":{
            "type":"integer"
        },
        "idnoticiaknewin":{
            "type":"integer"
        },
        "idfonte":{
            "type":"integer"
        },
        "nomefonte":{
          "type":"string"
        },
        "titulo":{
          "type":"string"
        },
        "subtitulo":{
          "type":"string"
        },
        "dominio":{
          "type":"string"
        },
        "autor":{
          "type":"string"
        },
        "conteudo":{
          "type":"string"
        },
        "url":{
          "type":"string"
        },
        "datapublicacao":{
            "type":"date",
            "format":"dd/MM/yyy HH:mm:ss"
        },
        "datacaptura_knewin":{
            "type":"date",
            "format":"dd/MM/yyy HH:mm:ss"
        },
        "datacaptura_zeeng":{
            "type":"date",
            "format":"dd/MM/yyy HH:mm:ss"
        },
        "categoria":{
            "type":"string"
        },
        "localidade":{
            "type":"string"
        },
        "linguagem":{
            "type":"string"
        },
          "imagens":{
              "properties":{
                  "titulo":{
                      "type":"string"
                  },
                  "url":{
                      "type":"string"
                  },
                  "creditos":{
                      "type":"string"
                  }
              }
          },
            "empresas":{
              "properties":{
                  "idnoticiaempresa":{
                      "type":"integer"
                  },
                  "idempresa":{
                      "type":"integer"
                  },
                  "nomeempresa":{
                      "type":"string"
                  },
                  "descricaoempresa":{
                      "type":"string"
                  }
              }
            },
            "termos":{
              "properties":{
                  "texto":{
                      "type":"string"
                  },
                  "frequencia":{
                      "type":"integer"
                  }
                }
            }
          }

      }
    }
}


#es.indices.delete(index='zeeng')
#es.indices.put_mapping(index='zeeng',body=mapping, doc_type='noticias' )
es.indices.create(index='zeeng',body=mapping )

