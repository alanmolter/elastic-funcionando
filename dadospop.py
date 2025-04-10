# Importa as bibliotecas necessárias
from elasticsearch import Elasticsearch, helpers  # Elasticsearch client e função para indexação em lote
import logging  # Para gerar logs do sistema (mensagens de info, erro, etc.)
import csv  # Para ler arquivos CSV
import json  # Para salvar dados JSON, útil em caso de falhas na indexação

# Configuração do sistema de logging
logging.basicConfig(level=logging.INFO)  # Define o nível mínimo de log como INFO
logger = logging.getLogger(__name__)  # Cria um logger com o nome do módulo atual

# Caminho para o arquivo CSV que será lido
CSV_FILE_PATH = "c2228.csv"

def connect_elastic():
    """
    Estabelece conexão com o Elasticsearch.
    Tenta se conectar ao serviço rodando localmente.
    """
    try:
        # Cria o cliente do Elasticsearch, apontando para localhost
        es = Elasticsearch(["http://localhost:9200"], request_timeout=30)

        # Testa se a conexão foi bem-sucedida
        if not es.ping():
            raise ValueError("Conexão com Elasticsearch falhou")

        logger.info("Conectado ao Elasticsearch")
        return es  # Retorna o objeto de conexão
    except Exception as e:
        logger.error(f"Erro ao conectar ao Elasticsearch: {str(e)}")
        exit(1)  # Encerra o programa com erro

def parse_csv_file(filepath):
    """
    Lê e converte os dados do arquivo CSV para uma lista de dicionários.
    Cada linha do CSV se torna um dicionário com chave sendo o nome da coluna.
    """
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)  # Cria um leitor que transforma cada linha em dicionário
        records = [dict(row) for row in reader]  # Converte cada linha em um dicionário
    logger.info(f"{len(records)} registros lidos do CSV.")  # Loga quantos registros foram lidos
    return records  # Retorna a lista de dicionários

def create_index(es, index_name, sample_record):
    """
    Cria um índice no Elasticsearch com base em um registro de exemplo (para gerar o mapeamento).
    Se o índice já existir, ele será excluído e recriado.
    """
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)  # Exclui o índice existente
        logger.info(f"Índice '{index_name}' excluído e será recriado.")

    # Cria o mapeamento dinâmico para os campos baseado no registro de exemplo
    properties = {}
    for key in sample_record.keys():
        properties[key] = {
            "type": "text",  # Tipo principal: texto
            "fields": {
                "keyword": {"type": "keyword", "ignore_above": 256}  # Subcampo para buscas exatas
            }
        }

    mappings = {
        "mappings": {
            "properties": properties
        }
    }

    es.indices.create(index=index_name, body=mappings)  # Cria o índice com o mapeamento gerado
    logger.info(f"Índice '{index_name}' criado com mapeamento automático.")

def index_csv_data(es, index_name, data):
    """
    Indexa os dados no Elasticsearch usando a função bulk para eficiência.
    Se houver falhas, salva os erros em um arquivo JSON.
    """
    try:
        # Cria ações de indexação para cada documento
        actions = [{"_index": index_name, "_source": doc} for doc in data]

        # Realiza a indexação em lote (bulk)
        response = helpers.bulk(es, actions, raise_on_error=False)

        failed = response[1]  # Documentos que falharam
        if failed:
            logger.error(f"Erro na indexação: {len(failed)} documentos falharam.")
            # Salva os documentos com erro em um arquivo JSON
            with open(f"failed_docs_{index_name}.json", "w", encoding="utf-8") as f:
                json.dump(failed, f, ensure_ascii=False, indent=4)
        else:
            logger.info(f"Indexação concluída: {len(data)} documentos indexados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao indexar dados: {str(e)}")

def main():
    """
    Função principal que orquestra as etapas:
    - Conexão com Elasticsearch
    - Leitura do CSV
    - Criação do índice
    - Indexação dos dados
    """
    es = connect_elastic()  # Conecta ao Elasticsearch
    index_name = "dados_csv"  # Nome do índice que será criado

    records = parse_csv_file(CSV_FILE_PATH)  # Lê os dados do CSV
    if records:
        create_index(es, index_name, records[0])  # Cria o índice com base no primeiro registro
        index_csv_data(es, index_name, records)  # Indexa os dados
    else:
        logger.warning("Nenhum registro encontrado para indexar.")

# Se o script for executado diretamente, chama a função main
if __name__ == "__main__":
    main()
