import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def convert_to_serializable(obj):
    """Converte tipos não serializáveis para formatos compatíveis com JSON"""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    elif isinstance(obj, np.generic):
        return obj.item()
    elif pd.isna(obj):
        return None
    return obj

def process_csv(file_path):
    try:
        # Detecta a codificação do arquivo para evitar erros na leitura
        with open(file_path, 'rb') as f:
            import chardet
            result = chardet.detect(f.read())
            encoding = result['encoding']

        df = pd.read_csv(file_path, delimiter=',', encoding=encoding, low_memory=False, dtype={'HORAOBITO': 'str'})

        # Conversão de datas - Utilize o formato correto "dd/mm/YYYY"
        date_columns = ['DTOBITO', 'DTNASC', 'DTATESTADO', 'DTCADASTRO', 'DTRECEBIM']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format="%d%m%Y", errors='coerce')

        # Conversão de coordenadas para geo_point
        if 'res_LATITUDE' in df.columns and 'res_LONGITUDE' in df.columns: # Utilizar os nomes corretos das colunas
            df['res_coordenadas'] = df.apply(lambda row: {'lat': row['res_LATITUDE'], 'lon': row['res_LONGITUDE']}
                                            if pd.notna(row['res_LATITUDE']) and pd.notna(row['res_LONGITUDE']) else None, axis=1)

        # Criando o campo data_obito formatado
        if 'DTOBITO' in df.columns:
            df['data_obito'] = df['DTOBITO'].apply(lambda x: x.isoformat() if pd.notna(x) else None)
        else:
            df['data_obito'] = None

        # Aplicar conversão serializável a todos os dados (isso já inclui a formatação isoformat() para datetime)
        for col in df.columns:
            df[col] = df[col].apply(convert_to_serializable)

        df.replace({np.nan: None, pd.NaT: None}, inplace=True)

        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Erro ao processar CSV: {str(e)}")
        exit(1)

def create_index(es, index_name):
    """Cria o índice com o mapeamento correto"""
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        logger.info(f"Índice {index_name} excluído e será recriado.")

    mappings = {
        "mappings": {
            "properties": {
                "ORIGEM": {"type": "integer"},
                "TIPOBITO": {"type": "integer"},
                "DTOBITO": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                "HORAOBITO": {"type": "keyword"},
                "NATURAL": {"type": "keyword"},
                "CODMUNNATU": {"type": "keyword"},
                "DTNASC": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                "IDADE": {"type": "integer"},
                "idade_obito_anos": {"type": "float"},
                "idade_obito_meses": {"type": "integer"},
                "idade_obito_dias": {"type": "integer"},
                "idade_obito_horas": {"type": "integer"},
                "idade_obito_mins": {"type": "integer"},
                "SEXO": {"type": "integer"},
                "RACACOR": {"type": "integer"},
                "ESTCIV": {"type": "integer"},
                "ESC": {"type": "integer"},
                "ESC2010": {"type": "integer"},
                "SERIESCFAL": {"type": "keyword"},
                "OCUP": {"type": "keyword"},
                "CODMUNRES": {"type": "keyword"},
                "LOCOCOR": {"type": "integer"},
                "CODESTAB": {"type": "keyword"},
                "ESTABDESCR": {"type": "keyword"},
                "CODMUNOCOR": {"type": "keyword"},
                "IDADEMAE": {"type": "integer"},
                "ESCMAE": {"type": "integer"},
                "ESCMAE2010": {"type": "integer"},
                "SERIESCMAE": {"type": "keyword"},
                "OCUPMAE": {"type": "keyword"},
                "QTDFILVIVO": {"type": "integer"},
                "QTDFILMORT": {"type": "integer"},
                "GRAVIDEZ": {"type": "integer"},
                "SEMAGESTAC": {"type": "float"},
                "GESTACAO": {"type": "keyword"},
                "PARTO": {"type": "integer"},
                "OBITOPARTO": {"type": "integer"},
                "PESO": {"type": "float"},
                "TPMORTEOCO": {"type": "integer"},
                "OBITOGRAV": {"type": "integer"},
                "OBITOPUERP": {"type": "integer"},
                "ASSISTMED": {"type": "integer"},
                "EXAME": {"type": "integer"},
                "CIRURGIA": {"type": "integer"},
                "NECROPSIA": {"type": "integer"},
                "LINHAA": {"type": "keyword"},
                "LINHAB": {"type": "keyword"},
                "LINHAC": {"type": "keyword"},
                "LINHAD": {"type": "keyword"},
                "LINHAII": {"type": "keyword"},
                "CAUSABAS": {"type": "keyword"},
                "CB_PRE": {"type": "keyword"},
                "COMUNSVOIM": {"type": "keyword"},
                "DTATESTADO": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                "CIRCOBITO": {"type": "integer"},
                "ACIDTRAB": {"type": "keyword"},
                "FONTE": {"type": "keyword"},
                "NUMEROLOTE": {"type": "keyword"},
                "TPPOS": {"type": "keyword"},
                "DTINVESTIG": {"type": "keyword"}, # Mantive keyword pois não há formato claro
                "CAUSABAS_O": {"type": "keyword"},
                "DTCADASTRO": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                "ATESTANTE": {"type": "keyword"},
                "STCODIFICA": {"type": "keyword"},
                "CODIFICADO": {"type": "keyword"},
                "VERSAOSIST": {"type": "keyword"},
                "VERSAOSCB": {"type": "keyword"},
                "FONTEINV": {"type": "keyword"},
                "DTRECEBIM": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                "ATESTADO": {"type": "keyword"},
                "DTRECORIGA": {"type": "keyword"}, # Mantive keyword pois não há formato claro
                "CAUSAMAT": {"type": "keyword"},
                "ESCMAEAGR1": {"type": "integer"},
                "ESCFALAGR1": {"type": "integer"},
                "STDOEPIDEM": {"type": "keyword"},
                "STDONOVA": {"type": "keyword"},
                "DIFDATA": {"type": "integer"},
                "NUDIASOBCO": {"type": "integer"},
                "NUDIASOBIN": {"type": "integer"},
                "DTCADINV": {"type": "keyword"}, # Mantive keyword pois não há formato claro
                "TPOBITOCOR": {"type": "integer"},
                "DTCONINV": {"type": "keyword"}, # Mantive keyword pois não há formato claro
                "FONTES": {"type": "keyword"},
                "TPRESGINFO": {"type": "keyword"},
                "TPNIVELINV": {"type": "keyword"},
                "NUDIASINF": {"type": "keyword"},
                "DTCADINF": {"type": "keyword"}, # Mantive keyword pois não há formato claro
                "MORTEPARTO": {"type": "keyword"},
                "DTCONCASO": {"type": "keyword"}, # Mantive keyword pois não há formato claro
                "FONTESINF": {"type": "keyword"},
                "ALTCAUSA": {"type": "keyword"},
                "CONTADOR": {"type": "integer"},
                "def_tipo_obito": {"type": "keyword"},
                "def_sexo": {"type": "keyword"},
                "def_raca_cor": {"type": "keyword"},
                "def_est_civil": {"type": "keyword"},
                "def_escol": {"type": "keyword"},
                "def_loc_ocor": {"type": "keyword"},
                "def_escol_mae": {"type": "keyword"},
                "def_gravidez": {"type": "keyword"},
                "def_gestacao": {"type": "keyword"},
                "def_parto": {"type": "keyword"},
                "def_obito_parto": {"type": "keyword"},
                "def_obito_grav": {"type": "keyword"},
                "def_obito_puerp": {"type": "keyword"},
                "def_assist_med": {"type": "keyword"},
                "def_exame": {"type": "keyword"},
                "def_cirurgia": {"type": "keyword"},
                "def_necropsia": {"type": "keyword"},
                "def_circ_obito": {"type": "keyword"},
                "def_acid_trab": {"type": "keyword"},
                "def_fonte": {"type": "keyword"},
                "res_MUNNOME": {"type": "keyword"},
                "res_MUNNOMEX": {"type": "keyword"},
                "res_AMAZONIA": {"type": "keyword"},
                "res_FRONTEIRA": {"type": "keyword"},
                "res_CAPITAL": {"type": "keyword"},
                "res_MSAUDCOD": {"type": "keyword"},
                "res_RSAUDCOD": {"type": "keyword"},
                "res_CSAUDCOD": {"type": "keyword"},
                "res_ANOEXT": {"type": "keyword"},
                "res_SUCESSOR": {"type": "keyword"},
                "res_LATITUDE": {"type": "float"},
                "res_LONGITUDE": {"type": "float"},
                "res_ALTITUDE": {"type": "integer"},
                "res_AREA": {"type": "float"},
                "res_codigo_adotado": {"type": "keyword"},
                "ocor_MUNNOME": {"type": "keyword"},
                "ocor_MUNNOMEX": {"type": "keyword"},
                "ocor_AMAZONIA": {"type": "keyword"},
                "ocor_FRONTEIRA": {"type": "keyword"},
                "ocor_CAPITAL": {"type": "keyword"},
                "ocor_MSAUDCOD": {"type": "keyword"},
                "ocor_RSAUDCOD": {"type": "keyword"},
                "ocor_CSAUDCOD": {"type": "keyword"},
                "ocor_ANOEXT": {"type": "keyword"},
                "ocor_SUCESSOR": {"type": "keyword"},
                "ocor_LATITUDE": {"type": "float"},
                "ocor_LONGITUDE": {"type": "float"},
                "ocor_ALTITUDE": {"type": "integer"},
                "ocor_AREA": {"type": "float"},
                "ocor_codigo_adotado": {"type": "keyword"},
                "ocor_SIGLA_UF": {"type": "keyword"},
                "ocor_CODIGO_UF": {"type": "integer"},
                "ocor_NOME_UF": {"type": "keyword"},
                "res_SIGLA_UF": {"type": "keyword"},
                "res_CODIGO_UF": {"type": "integer"},
                "res_NOME_UF": {"type": "keyword"},
                "ocor_REGIAO": {"type": "keyword"},
                "res_REGIAO": {"type": "keyword"},
                "causabas_capitulo": {"type": "keyword"},
                "causabas_grupo": {"type": "keyword"},
                "causabas_categoria": {"type": "keyword"},
                "causabas_subcategoria": {"type": "keyword"},
                "ocor_coordenadas": {"type": "geo_point"},
                "res_coordenadas": {"type": "geo_point"},
                "data_obito": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                "dia_semana_obito": {"type": "keyword"},
                "ano_obito": {"type": "integer"},
                "data_nasc": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                "dia_semana_nasc": {"type": "keyword"},
                "ano_nasc": {"type": "integer"},
                "idade_obito": {"type": "float"},
                "idade_obito_calculado": {"type": "float"},
            }
        }
    }

    es.indices.create(index=index_name, body=mappings)
    logger.info(f"Índice '{index_name}' criado com mapeamento definido.")

def index_data(es, index_name, data):
    """Indexa os dados no Elasticsearch"""
    try:
        actions = [{"_index": index_name, "_source": doc} for doc in data]
        response = helpers.bulk(es, actions, raise_on_error=False)

        failed = response[1]
        if failed:
            logger.error(f"Erro na indexação: {len(failed)} documentos falharam.")
            with open("failed_docs.json", "w", encoding="utf-8") as f:
                json.dump(failed, f, ensure_ascii=False, indent=4)
        else:
            logger.info(f"Indexação concluída: {len(data)} documentos indexados com sucesso")
    except Exception as e:
        logger.error(f"Erro na indexação: {str(e)}")

def main():
    es = connect_elastic()
    index_name = "obitos"

    create_index(es, index_name)
    data = process_csv("dados_obitos.csv")
    index_data(es, index_name, data)

if __name__ == "__main__":
    main()