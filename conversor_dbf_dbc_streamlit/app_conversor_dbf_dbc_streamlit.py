
"""
CONVERSOR DBF/DBC → CSV — STREAMLIT

Aplicação web simples para converter arquivos .DBF e .DBC do DATASUS em .CSV.

Instalação local:
    pip install -r requirements.txt

Execução local:
    streamlit run app_conversor_dbf_dbc_streamlit.py

Publicação no Streamlit Cloud:
    Suba para o GitHub:
        app_conversor_dbf_dbc_streamlit.py
        requirements.txt
        packages.txt

Observação:
- DBF é lido diretamente com dbfread.
- DBC é primeiro convertido para DBF usando readdbc ou pyreaddbc.
- Depois o DBF convertido é lido e exportado como CSV.
"""

from __future__ import annotations

import csv
import io
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from dbfread import DBF


# ============================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================

st.set_page_config(
    page_title="Conversor DBF/DBC para CSV",
    page_icon="📄",
    layout="wide",
)


# ============================================================
# FUNÇÕES
# ============================================================

def salvar_uploads_em_pasta_temp(uploaded_files, pasta: Path) -> list[Path]:
    """
    Salva arquivos enviados em pasta temporária.

    Isso permite que:
    - DBF seja lido pelo dbfread;
    - DBC seja convertido para DBF;
    - arquivos memo .dbt/.fpt fiquem no mesmo diretório.
    """
    caminhos = []

    for uploaded in uploaded_files:
        caminho = pasta / uploaded.name
        caminho.write_bytes(uploaded.getvalue())
        caminhos.append(caminho)

    return caminhos


def normalizar_valor(valor: Any) -> Any:
    """
    Mantém o dado o mais bruto possível, apenas trocando None por vazio.
    """
    if valor is None:
        return ""
    return valor


def converter_dbc_para_dbf(dbc_path: Path, destino_dir: Path) -> Path:
    """
    Converte DBC para DBF.

    Tenta primeiro readdbc:
        import readdbc
        readdbc.dbc2dbf(entrada, saida)

    Se falhar, tenta pyreaddbc:
        from pyreaddbc import dbc2dbf
        dbc2dbf(entrada, saida)

    Retorna o caminho do DBF convertido.
    """
    dbf_path = destino_dir / f"{dbc_path.stem}.dbf"
    erros = []

    try:
        import readdbc  # type: ignore

        if hasattr(readdbc, "dbc2dbf"):
            readdbc.dbc2dbf(str(dbc_path), str(dbf_path))
            if dbf_path.exists() and dbf_path.stat().st_size > 0:
                return dbf_path
            erros.append("readdbc.dbc2dbf executou, mas não gerou DBF válido.")
        else:
            erros.append("Pacote readdbc importado, mas função dbc2dbf não encontrada.")
    except Exception as e:
        erros.append(f"readdbc falhou: {e}")

    try:
        from pyreaddbc import dbc2dbf  # type: ignore

        dbc2dbf(str(dbc_path), str(dbf_path))
        if dbf_path.exists() and dbf_path.stat().st_size > 0:
            return dbf_path
        erros.append("pyreaddbc.dbc2dbf executou, mas não gerou DBF válido.")
    except Exception as e:
        erros.append(f"pyreaddbc falhou: {e}")

    msg = (
        "Não foi possível converter o arquivo DBC para DBF. "
        "Verifique se as dependências readdbc/pyreaddbc foram instaladas corretamente. "
        "Detalhes: " + " | ".join(erros)
    )
    raise RuntimeError(msg)


def ler_dbf_para_dataframe(
    dbf_path: Path,
    encoding: str = "latin-1",
    char_decode_errors: str = "ignore",
) -> pd.DataFrame:
    """
    Lê DBF e retorna DataFrame.
    """
    tabela = DBF(
        str(dbf_path),
        encoding=encoding,
        char_decode_errors=char_decode_errors,
        load=True,
    )

    registros = []
    for registro in tabela:
        registros.append({k: normalizar_valor(v) for k, v in dict(registro).items()})

    return pd.DataFrame(registros, columns=tabela.field_names)


def converter_arquivo_para_dataframe(
    caminho: Path,
    pasta_temp: Path,
    encoding: str,
    char_decode_errors: str,
) -> tuple[pd.DataFrame, str]:
    """
    Converte DBF ou DBC para DataFrame.

    Retorna:
        dataframe, tipo_processamento
    """
    sufixo = caminho.suffix.lower()

    if sufixo == ".dbf":
        df = ler_dbf_para_dataframe(
            dbf_path=caminho,
            encoding=encoding,
            char_decode_errors=char_decode_errors,
        )
        return df, "DBF lido diretamente"

    if sufixo == ".dbc":
        dbf_convertido = converter_dbc_para_dbf(caminho, pasta_temp)
        df = ler_dbf_para_dataframe(
            dbf_path=dbf_convertido,
            encoding=encoding,
            char_decode_errors=char_decode_errors,
        )
        return df, "DBC convertido para DBF e depois lido"

    raise ValueError(f"Tipo de arquivo não suportado: {caminho.name}")


def dataframe_para_csv_bytes(df: pd.DataFrame, separador: str = ";") -> bytes:
    """
    Exporta DataFrame para CSV em UTF-8 com BOM, adequado para Excel.
    """
    buffer = io.StringIO()
    df.to_csv(
        buffer,
        index=False,
        sep=separador,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )
    return buffer.getvalue().encode("utf-8-sig")


def montar_zip_csv(resultados: list[dict], separador: str) -> bytes:
    """
    Gera ZIP com todos os CSVs convertidos.
    """
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for item in resultados:
            csv_bytes = dataframe_para_csv_bytes(item["df"], separador=separador)
            zf.writestr(item["nome_csv"], csv_bytes)

    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def detectar_arquivos_convertiveis(caminhos: list[Path]) -> list[Path]:
    """
    Retorna arquivos .dbf e .dbc enviados.
    """
    return sorted(
        [
            p for p in caminhos
            if p.suffix.lower() in {".dbf", ".dbc"}
        ]
    )


def checar_suporte_dbc() -> dict:
    """
    Verifica quais bibliotecas de DBC estão disponíveis.
    """
    status = {
        "readdbc": False,
        "pyreaddbc": False,
        "mensagens": [],
    }

    try:
        import readdbc  # noqa: F401
        status["readdbc"] = True
    except Exception as e:
        status["mensagens"].append(f"readdbc indisponível: {e}")

    try:
        import pyreaddbc  # noqa: F401
        status["pyreaddbc"] = True
    except Exception as e:
        status["mensagens"].append(f"pyreaddbc indisponível: {e}")

    return status


# ============================================================
# INTERFACE
# ============================================================

st.title("📄 Conversor DBF/DBC → CSV")
st.caption("Envie arquivos `.dbf` ou `.dbc` do DATASUS e baixe os dados convertidos em `.csv`.")

with st.expander("Como usar", expanded=True):
    st.markdown(
        """
        1. Envie um ou mais arquivos `.dbf` ou `.dbc`.
        2. Escolha o encoding.
        3. Clique em **Converter**.
        4. Baixe os CSVs individualmente ou todos juntos em ZIP.

        **DBF** é lido diretamente.  
        **DBC** é convertido para DBF e depois exportado como CSV.
        """
    )

status_dbc = checar_suporte_dbc()

with st.expander("Diagnóstico de suporte a DBC", expanded=False):
    st.write(
        {
            "readdbc_disponivel": status_dbc["readdbc"],
            "pyreaddbc_disponivel": status_dbc["pyreaddbc"],
        }
    )
    if status_dbc["mensagens"]:
        st.code("\n".join(status_dbc["mensagens"]))

if not status_dbc["readdbc"] and not status_dbc["pyreaddbc"]:
    st.warning(
        "O suporte a DBC depende de `readdbc` ou `pyreaddbc`. "
        "Se você enviar um .dbc e a conversão falhar, verifique o requirements.txt e o packages.txt."
    )

col_a, col_b, col_c = st.columns([1, 1, 1])

with col_a:
    encoding = st.selectbox(
        "Encoding do DBF/DBC",
        ["latin-1", "cp850", "cp1252", "utf-8"],
        index=0,
        help="Para bases do DATASUS/SINAN, latin-1 ou cp850 costumam funcionar melhor.",
    )

with col_b:
    separador = st.selectbox(
        "Separador do CSV",
        [",", ";"],
        index=1,
        format_func=lambda x: "Ponto e vírgula (;)" if x == ";" else "Vírgula (,)",
        help="No Brasil, ponto e vírgula costuma abrir melhor no Excel.",
    )

with col_c:
    char_decode_errors = st.selectbox(
        "Tratamento de erro de caracteres",
        ["ignore", "replace", "strict"],
        index=0,
        help="Use 'ignore' quando houver erro de encoding.",
    )

uploaded_files = st.file_uploader(
    "Arquivos DBF/DBC",
    type=["dbf", "DBF", "dbc", "DBC", "dbt", "DBT", "fpt", "FPT"],
    accept_multiple_files=True,
)

if not uploaded_files:
    st.info("Envie pelo menos um arquivo `.dbf` ou `.dbc` para começar.")
    st.stop()

st.write(f"Arquivos enviados: **{len(uploaded_files)}**")

with st.expander("Arquivos recebidos", expanded=False):
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "arquivo": f.name,
                    "tipo": Path(f.name).suffix.lower(),
                    "tamanho_kb": round(len(f.getvalue()) / 1024, 2),
                }
                for f in uploaded_files
            ]
        ),
        use_container_width=True,
    )

if "resultados_conversao_dbf_dbc" not in st.session_state:
    st.session_state["resultados_conversao_dbf_dbc"] = None
    st.session_state["erros_conversao_dbf_dbc"] = None

if st.button("Converter", type="primary"):
    resultados = []
    erros = []

    progress = st.progress(0)
    status = st.empty()

    with tempfile.TemporaryDirectory() as tmp:
        pasta_tmp = Path(tmp)
        caminhos = salvar_uploads_em_pasta_temp(uploaded_files, pasta_tmp)
        arquivos_convertiveis = detectar_arquivos_convertiveis(caminhos)

        if not arquivos_convertiveis:
            st.error("Nenhum arquivo `.dbf` ou `.dbc` foi encontrado entre os arquivos enviados.")
            st.stop()

        for i, caminho in enumerate(arquivos_convertiveis, start=1):
            status.info(f"Convertendo {caminho.name}...")

            try:
                df, processamento = converter_arquivo_para_dataframe(
                    caminho=caminho,
                    pasta_temp=pasta_tmp,
                    encoding=encoding,
                    char_decode_errors=char_decode_errors,
                )

                nome_csv = f"{caminho.stem}.csv"

                resultados.append(
                    {
                        "arquivo_origem": caminho.name,
                        "tipo_origem": caminho.suffix.lower(),
                        "processamento": processamento,
                        "nome_csv": nome_csv,
                        "linhas": df.shape[0],
                        "colunas": df.shape[1],
                        "df": df,
                    }
                )

            except Exception as e:
                erros.append(
                    {
                        "arquivo": caminho.name,
                        "erro": str(e),
                    }
                )

            progress.progress(i / len(arquivos_convertiveis))

    status.success("Conversão finalizada.")
    st.session_state["resultados_conversao_dbf_dbc"] = resultados
    st.session_state["erros_conversao_dbf_dbc"] = erros

resultados = st.session_state.get("resultados_conversao_dbf_dbc")
erros = st.session_state.get("erros_conversao_dbf_dbc")

if resultados is not None:
    st.divider()
    st.subheader("Resultado da conversão")

    if resultados:
        resumo = pd.DataFrame(
            [
                {
                    "arquivo_origem": item["arquivo_origem"],
                    "tipo": item["tipo_origem"],
                    "processamento": item["processamento"],
                    "csv_gerado": item["nome_csv"],
                    "linhas": item["linhas"],
                    "colunas": item["colunas"],
                }
                for item in resultados
            ]
        )

        st.success(f"{len(resultados)} arquivo(s) convertido(s) com sucesso.")
        st.dataframe(resumo, use_container_width=True)

        st.subheader("Pré-visualização")

        nomes = [item["nome_csv"] for item in resultados]
        escolhido = st.selectbox("Escolha um CSV para visualizar", nomes)
        item_escolhido = next(item for item in resultados if item["nome_csv"] == escolhido)

        st.dataframe(item_escolhido["df"].head(1000), use_container_width=True, height=500)

        st.subheader("Downloads")

        if len(resultados) == 1:
            item = resultados[0]
            st.download_button(
                label=f"Baixar {item['nome_csv']}",
                data=dataframe_para_csv_bytes(item["df"], separador=separador),
                file_name=item["nome_csv"],
                mime="text/csv",
            )
        else:
            zip_bytes = montar_zip_csv(resultados, separador=separador)
            st.download_button(
                label="Baixar todos em ZIP",
                data=zip_bytes,
                file_name="dbf_dbc_convertidos_csv.zip",
                mime="application/zip",
            )

            with st.expander("Downloads individuais", expanded=False):
                for item in resultados:
                    st.download_button(
                        label=f"Baixar {item['nome_csv']}",
                        data=dataframe_para_csv_bytes(item["df"], separador=separador),
                        file_name=item["nome_csv"],
                        mime="text/csv",
                        key=f"download_{item['nome_csv']}",
                    )

    if erros:
        st.error(f"{len(erros)} arquivo(s) apresentaram erro.")
        st.dataframe(pd.DataFrame(erros), use_container_width=True)

st.divider()
st.caption("Conversor DBF/DBC → CSV em Streamlit. Os arquivos são processados temporariamente durante a sessão.")
