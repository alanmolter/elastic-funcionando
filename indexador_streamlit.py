import streamlit as st
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import json
import io

# Conecta ao Elasticsearch
@st.cache_resource
def connect_elastic():
    es = Elasticsearch(["http://localhost:9200"], request_timeout=30)
    if not es.ping():
        st.error("❌ Falha na conexão com o Elasticsearch.")
        st.stop()
    return es

# Cria índice dinamicamente com base em um DataFrame
def create_index(es, index_name, sample_record):
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        st.info(f"Índice '{index_name}' já existia e foi recriado.")

    properties = {
        col: {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        } for col in sample_record.keys()
    }

    mappings = {
        "mappings": {
            "properties": properties
        }
    }

    es.indices.create(index=index_name, body=mappings)
    st.success(f"Índice '{index_name}' criado com sucesso!")

# Indexa os dados no Elasticsearch
def index_data(es, index_name, records):
    actions = [{"_index": index_name, "_source": rec} for rec in records]
    response = helpers.bulk(es, actions, raise_on_error=False)

    failed = response[1]
    if failed:
        st.error(f"🚫 {len(failed)} documentos falharam ao ser indexados.")
        st.download_button(
            label="📥 Baixar erros como JSON",
            data=json.dumps(failed, ensure_ascii=False, indent=4),
            file_name=f"failed_docs_{index_name}.json",
            mime="application/json"
        )
    else:
        st.success(f"✅ Todos os {len(records)} documentos foram indexados com sucesso!")

# Interface Streamlit
def main():
    st.title("📊 Indexador de CSV no Elasticsearch")
    st.write("Faça upload de um arquivo `.csv` para indexar seus dados no Elasticsearch.")

    uploaded_file = st.file_uploader("Selecione o arquivo CSV", type=["csv"])
    index_name = st.text_input("Nome do índice no Elasticsearch", value="meu_indice")

    if uploaded_file and index_name:
        # Lê o CSV como DataFrame
        try:
            df = pd.read_csv(uploaded_file)
            st.dataframe(df.head())  # Exibe os primeiros dados
        except Exception as e:
            st.error(f"Erro ao ler CSV: {e}")
            return

        if st.button("📤 Indexar no Elasticsearch"):
            es = connect_elastic()
            records = df.to_dict(orient="records")

            if records:
                create_index(es, index_name, records[0])
                index_data(es, index_name, records)
            else:
                st.warning("⚠️ O CSV está vazio.")

if __name__ == "__main__":
    main()
