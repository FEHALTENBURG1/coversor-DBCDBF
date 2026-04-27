
# Conversor DBF/DBC -> CSV em Streamlit

Aplicação web simples para converter arquivos `.dbf` e `.dbc` do DATASUS em `.csv`.

## Como rodar localmente

```bash
pip install -r requirements.txt
streamlit run app_conversor_dbf_dbc_streamlit.py
```

## Como usar

1. Abra a aplicação.
2. Envie um ou mais arquivos `.dbf` ou `.dbc`.
3. Escolha o encoding.
4. Clique em **Converter**.
5. Baixe o CSV ou o ZIP com todos os arquivos convertidos.

## Como funciona

- Arquivos `.dbf` são lidos diretamente com `dbfread`.
- Arquivos `.dbc` são convertidos para `.dbf` usando `readdbc` ou `pyreaddbc`.
- Depois o `.dbf` é exportado como `.csv`.

## Publicação no Streamlit Community Cloud

Suba para o GitHub:

- `app_conversor_dbf_dbc_streamlit.py`
- `requirements.txt`
- `packages.txt`

O arquivo `packages.txt` ajuda a instalar dependências de sistema necessárias para compilar algumas bibliotecas de DBC.
