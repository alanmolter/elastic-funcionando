from elasticsearch import Elasticsearch, helpers
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dados dos estados
estados_data = """COD,NOME,SIGLA
35,São Paulo, SP
41,Paraná, PR
42,Santa Catarina, SC
43,Rio Grande do Sul, RS
50,Mato Grosso do Sul, MS
11,Rondônia, RO
12,Acre, AC
13,Amazonas, AM
14,Roraima, RR
15,Pará, PA
16,Amapá, AP
17,Tocantins, TO
21,Maranhão, MA
24,Rio Grande do Norte, RN
25,Paraíba, PB
26,Pernambuco, PE
27,Alagoas, AL
28,Sergipe, SE
29,Bahia, BA
31,Minas Gerais, MG
33,Rio de Janeiro, RJ
51,Mato Grosso, MT
52,Goiás, GO
53,Distrito Federal, DF
22,Piauí, PI
23,Ceará, CE
32,Espírito Santo, ES
"""

def connect_elastic():
    """Estabelece conexão com o Elasticsearch"""
    try:
        es = Elasticsearch(["http://localhost:9200"], timeout=30)
        if not es.ping():
            raise ValueError("Conexão com Elasticsearch falhou")
        logger.info("Conectado ao Elasticsearch")
        return es
    except Exception as e:
        logger.error(f"Erro ao conectar ao Elasticsearch: {str(e)}")
        exit(1)

def parse_estados_data(data):
    """Parses the raw data dos estados em uma lista de dicionários."""
    lines = data.strip().split('\n')
    headers = [header.strip().lower() for header in lines[0].split(',')]
    records = []
    for line in lines[1:]:
        values = [value.strip() for value in line.split(',')]
        record = dict(zip(headers, values))
        records.append(record)
    return records

def create_estados_index(es, index_name="estados"):
    """Cria o índice de estados com um mapeamento básico"""
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        logger.info(f"Índice '{index_name}' excluído e será recriado.")

    mappings = {
        "mappings": {
            "properties": {
                "cod": {"type": "keyword"},
                "nome": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "sigla": {"type": "keyword"}
            }
        }
    }

    es.indices.create(index=index_name, body=mappings)
    logger.info(f"Índice '{index_name}' criado com mapeamento definido.")

def index_estados_data(es, index_name, data):
    """Indexa os dados dos estados no Elasticsearch"""
    try:
        actions = [{"_index": index_name, "_source": doc} for doc in data]
        response = helpers.bulk(es, actions, raise_on_error=False)

        failed = response[1]
        if failed:
            logger.error(f"Erro na indexação de '{index_name}': {len(failed)} documentos falharam.")
            with open(f"failed_docs_{index_name}.json", "w", encoding="utf-8") as f:
                json.dump(failed, f, ensure_ascii=False, indent=4)
        else:
            logger.info(f"Indexação de '{index_name}' concluída: {len(data)} documentos indexados com sucesso")
    except Exception as e:
        logger.error(f"Erro na indexação de '{index_name}': {str(e)}")

def main():
    es = connect_elastic()
    index_name = "estados"

    create_estados_index(es, index_name)
    estados_records = parse_estados_data(estados_data)
    index_estados_data(es, index_name, estados_records)

if __name__ == "__main__":
    main()