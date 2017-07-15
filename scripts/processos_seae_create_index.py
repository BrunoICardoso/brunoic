from datetime import datetime
from elasticsearch import Elasticsearch

# es = Elasticsearch(['172.16.39.5:9200'])
# es = Elasticsearch(['172.16.0.85:9200'])
es = Elasticsearch(['187.103.103.11:9200'])

mapping = {
"mappings":{
    "processos_seae":{
      "properties":{
        "idprocesso":{
            "type":"integer"
        },
        "numprocesso":{
            "type":"string"
        },
        "dtprocesso":{
            "type":"date",
            "format":"yyyy-MM-dd"
        },
        "dtvigenciaini":{
            "type":"date",
            "format":"yyyy-MM-dd"
        },
        "dtvigenciafim":{
            "type":"date",
            "format":"yyyy-MM-dd"
        },
        "interessados":{
          "type":"string"
        },
        "premios":{
          "type":"string"
        },
        "valortotalpremios":{
          "type":"double"
        },
        "modalidade":{
          "type":"string"
        },
        "formacontemplacao":{
          "type":"string"
        },
        "abrangencia_nacional":{
          "type":"boolean"
        },
        "numdocs":{
          "type":"integer"
        },
        "nome":{
            "type":"string"
        },
        "resumo":{
            "type":"string"
        },
        "comoparticipar":{
            "type":"string"
        },
        "situacaoatual":{
            "type":"string",
            "fielddata":"true",
            "fields": { "raw" : { "type": "string", "index": "not_analyzed" } }
        },
        "datasituacaoatual":{
            "type":"date",
            "format": "yyyy-MM-dd"
        },
        "idsituacaoatual":{
            "type":"integer"
        },
          "abrangestados":{
              "properties":{
                  "idestado":{
                      "type":"integer"
                  },
                  "nome":{
                      "type":"string"
                  },
                  "uf":{
                      "type":"string"
                  }
              }
          },
          "abrangmunicipios":{
              "properties":{
                  "idmunicipio":{
                      "type":"integer"
                  },
                  "nome":{
                      "type":"string"
                  },
                  "uf":{
                      "type":"string"
                  }
              }
          },
        "empresas":{
              "properties":{
                  "idempresa":{
                      "type":"integer"
                  },
                  "nome":{
                      "type":"string"
                  }
              }
          },
          "setores":{
              "properties":{
                  "idsetor":{
                      "type":"integer"
                  },
                  "codsetor":{
                      "type":"string"
                  },
                  "descsetor":{
                      "type":"string"
                  },
                  "idsubsetor": {
                      "type": "integer"
                  },
                  "codsubsetor": {
                      "type": "string"
                  },
                  "descsubsetor": {
                      "type": "string"
                  },
              }
          },
           "situacoes":{
              "properties":{
                  "idsituacao":{
                      "type":"integer"
                  },
                  "descricao":{
                      "type":"string"
                  },
                  "data": {
                      "type": "date",
                      "format": "yyyy-MM-dd"
                  }
              }
            },
            "arquivos":{
              "properties":{
                  "idarquivo":{
                      "type":"integer"
                  },
                  "numdoc":{
                      "type":"string"
                  },
                  "coordenacao":{
                      "type":"string"
                  },"situacao":{
                      "type":"string"
                  },
                  "link":{
                      "type":"string"
                  },
                  "nomearquivo":{
                      "type":"string"
                  },
                  "textoarquivo":{
                      "type":"string"
                  }
                }
            },
            "solicitantes":{
              "properties":{
                  "idsolicitante":{
                      "type":"integer"
                  },
                  "solicitante":{
                      "type":"string"
                  },
                  "cnpj":{
                      "type":"string"
                  }
                }
            }
          }

      }
    }
}


# es.delete(index='promocoes',doc_type='processos_seae');

es.indices.delete(index='promocoes')

# es.indices.put_mapping(index='promocoes',body=mapping, doc_type='processos_seae' )
es.indices.create(index='promocoes', body=mapping )
