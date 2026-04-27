# Conversor DBF/DBC -> CSV em Streamlit — versão Cloud fix

Esta versão foi ajustada para reduzir erro de instalação no Streamlit Community Cloud.

## Arquivos para subir no GitHub

- `app_conversor_dbf_dbc_streamlit.py`
- `requirements.txt`
- `packages.txt`

## Configuração importante no Streamlit Cloud

Ao criar o app, abra **Advanced settings** e selecione **Python 3.11** ou **Python 3.12**.

Se o app já foi criado com Python 3.13, delete e publique novamente selecionando Python 3.11/3.12. Apenas alterar `requirements.txt` pode não mudar a versão do Python do ambiente.

## requirements.txt usado nesta versão

```txt
streamlit
pandas
dbfread
cffi==1.17.1
readdbc
```

## packages.txt usado nesta versão

```txt
build-essential
libffi-dev
python3-dev
```

## Observação

Removi `pyreaddbc` do requirements porque ele aumenta a chance de falha de build. O app ainda mantém a lógica de DBF e tenta DBC com `readdbc`.
