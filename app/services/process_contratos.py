import pandas as pd
import io
import os
import re
import logging
from typing import List
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from openpyxl.utils import get_column_letter

# Configurar logging para debug
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# ========================================
# NOMES PADRONIZADOS DAS ABAS
# ========================================
def get_sheet_names(bank_type: str) -> dict:
    """
    Retorna os nomes padronizados das abas baseado no banco.
    """
    if bank_type == 'bemge':
        return {
            '3026-11': 'Bemge 3026-11-Habil.Não Homol.',
            '3026-12-AUD': 'Bemge 3026-12-Homol.Auditados',
            '3026-12-NAUD': 'Bemge 3026-12-Homol.Não Auditados',
            '3026-15': 'Bemge 3026-15-Homol.Neg.Cob'
        }
    else:  # minas_caixa
        return {
            '3026-11': 'Minas Caixa 3026-11-Habil.Não Homol',
            '3026-12-AUD': 'Minas Caixa 3026-12-Homol. Auditado',
            '3026-12-NAUD': 'Minas Caixa 3026-12-Homol.Não Auditado',
            '3026-15': 'Minas Caixa 3026-15-Homol.Neg.Cob'
        }


def detect_file_type(filename: str) -> str:
    """
    Detecta o tipo de arquivo baseado no nome.
    Retorna: '3026-11', '3026-12' ou '3026-15'
    """
    filename_upper = filename.upper()
    if '3026-11' in filename_upper or '302611' in filename_upper:
        return '3026-11'
    elif '3026-12' in filename_upper or '302612' in filename_upper:
        return '3026-12'
    elif '3026-15' in filename_upper or '302615' in filename_upper:
        return '3026-15'
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Tipo de arquivo não reconhecido: {filename}. Esperado: 3026-11, 3026-12 ou 3026-15"
        )


def get_bank_name(bank_type: str) -> str:
    """Converte bank_type para nome do banco"""
    bank_names = {
        'bemge': 'BEMGE',
        'minas_caixa': 'MINAS CAIXA'
    }
    return bank_names.get(bank_type, bank_type.upper())


def format_contrato_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formata a coluna CONTRATO como texto, preservando zeros à esquerda.
    """
    if 'CONTRATO' in df.columns:
        df['CONTRATO'] = df['CONTRATO'].apply(
            lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x) if pd.notna(x) else ''
        )
    return df


def format_column_d_as_text(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formata a coluna D (índice 3) como texto.
    """
    if len(df.columns) > 3:
        coluna_d = df.columns[3]
        df[coluna_d] = df[coluna_d].apply(
            lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x) if pd.notna(x) else ''
        )
    return df


def format_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formata colunas de data, removendo a hora.
    Colunas afetadas: DT.ASS., DT.EVENTO, DT.HAB., DT.PROC.HAB. e variações
    """
    # Lista de possíveis nomes de colunas de data
    colunas_data = [
        'DT.ASS.', 'DT.EVENTO', 'DT.HAB.', 'DT.PROC.HAB.',
        'DT.ASS', 'DT.EVENTO', 'DT.HAB', 'DT.PROC.HAB',
        'DATA ASS.', 'DATA EVENTO', 'DATA HAB.', 'DATA PROC.HAB.',
        'DT.BASE', 'DT.TERM.ANALISE', 'DT.MANIFESTACAO', 'DT.POS.NOVACAO',
        'DT.ULT.AUDITORIA', 'DT.ULT.NEGOCIACAO', 'DATA STATUS'
    ]
    
    for col in df.columns:
        # Verificar se a coluna está na lista ou começa com DT. ou DATA
        if col in colunas_data or col.upper().startswith('DT.') or col.upper().startswith('DATA'):
            try:
                # Converter para datetime e extrair apenas a data
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                logger.debug(f"Coluna de data formatada: {col}")
            except Exception as e:
                logger.warning(f"Erro ao formatar coluna de data {col}: {e}")
    
    return df


def format_date_columns_by_index(df: pd.DataFrame, indices: list) -> pd.DataFrame:
    """
    Formata colunas de data específicas por índice (0-based), removendo a hora.
    Usado para MINAS CAIXA 3026-11 colunas T, X, Z (índices 19, 23, 25).
    """
    for idx in indices:
        if len(df.columns) > idx:
            col = df.columns[idx]
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                logger.debug(f"Coluna de data (índice {idx}) formatada: {col}")
            except Exception as e:
                logger.warning(f"Erro ao formatar coluna de data índice {idx} ({col}): {e}")
    
    return df


def remove_general_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas AF, AG, AH (INDVAF3TR7, INDVAF4TR7, DT.ULT.HOMOLOGACAO).
    """
    colunas_remover_geral = ['INDVAF3TR7', 'INDVAF4TR7', 'DT.ULT.HOMOLOGACAO']
    for col in colunas_remover_geral:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df


def filter_last_2_months(df: pd.DataFrame, date_column: str = None) -> pd.DataFrame:
    """
    Filtra contratos dos últimos 2 meses baseado na coluna de data de manifestação.
    Coluna AG (índice 32) = DT.MANIFESTACAO
    """
    # Tentar encontrar a coluna de data de manifestação
    possible_cols = ['DT.MANIFESTACAO', 'DT.MANIFESTAÇÃO', 'DATA MANIFESTACAO', 'DATA MANIFESTAÇÃO']
    
    col_manifestacao = None
    if date_column and date_column in df.columns:
        col_manifestacao = date_column
    else:
        for col in possible_cols:
            if col in df.columns:
                col_manifestacao = col
                break
        
        # Se não encontrou por nome, tentar por índice (AG = índice 32)
        if col_manifestacao is None and len(df.columns) > 32:
            col_manifestacao = df.columns[32]
            logger.debug(f"Usando coluna índice 32 para filtro de data: {col_manifestacao}")
    
    if col_manifestacao is None:
        logger.warning("Coluna de data de manifestação não encontrada para filtro de 2 meses")
        return df
    
    try:
        # Calcular data de 2 meses atrás
        hoje = datetime.now().date()
        dois_meses_atras = hoje - relativedelta(months=2)
        
        logger.debug(f"Filtrando contratos a partir de {dois_meses_atras}")
        
        # Converter coluna para datetime se ainda não estiver
        df_temp = df.copy()
        if df_temp[col_manifestacao].dtype == 'object' or not hasattr(df_temp[col_manifestacao].iloc[0] if len(df_temp) > 0 else None, 'year'):
            df_temp[col_manifestacao] = pd.to_datetime(df_temp[col_manifestacao], errors='coerce')
        
        # Filtrar últimos 2 meses
        mask = df_temp[col_manifestacao] >= pd.Timestamp(dois_meses_atras)
        df_filtrado = df[mask].copy()
        
        logger.debug(f"Filtro últimos 2 meses: {len(df)} -> {len(df_filtrado)} linhas")
        
        return df_filtrado
        
    except Exception as e:
        logger.error(f"Erro ao filtrar últimos 2 meses: {e}")
        return df


def process_3026_11(df: pd.DataFrame, bank_name: str) -> tuple:
    """
    Processa arquivos 3026-11.
    - Formata coluna D como texto
    - Remove duplicados na coluna CONTRATO
    - Para MINAS CAIXA: formata colunas T, X, Z (índices 19, 23, 25) removendo horas
    Retorna: (df_processado, total_linhas, total_unicos, total_duplicados)
    """
    logger.debug(f"Processando 3026-11 para {bank_name} - Linhas: {len(df)}, Colunas: {len(df.columns)}")
    logger.debug(f"Colunas do 3026-11: {list(df.columns)[:10]}...")
    
    # Verificar se DataFrame está vazio
    if df.empty:
        logger.warning(f"DataFrame 3026-11 está vazio para {bank_name}")
        return pd.DataFrame(), 0, 0, 0
    
    # Fazer cópia para evitar problemas de referência
    df = df.copy()
    
    # Para MINAS CAIXA: formatar colunas T, X, Z (índices 19, 23, 25) removendo horas
    if 'MINAS' in bank_name.upper():
        logger.debug("Aplicando formatação de data para colunas T, X, Z (MINAS CAIXA)")
        df = format_date_columns_by_index(df, [19, 23, 25])
    
    # Formatar coluna D como texto
    df = format_column_d_as_text(df)
    
    # Verificar se tem coluna CONTRATO (tentar variações)
    coluna_contrato = None
    for nome in ['CONTRATO', 'CONTRATO_NUM', 'NUM_CONTRATO', 'NR_CONTRATO']:
        if nome in df.columns:
            coluna_contrato = nome
            break
    
    if coluna_contrato is None:
        logger.error(f"Coluna CONTRATO não encontrada. Colunas disponíveis: {list(df.columns)}")
        raise HTTPException(
            status_code=400,
            detail="Coluna 'CONTRATO' não encontrada no arquivo 3026-11"
        )
    
    # Renomear para CONTRATO se necessário
    if coluna_contrato != 'CONTRATO':
        df = df.rename(columns={coluna_contrato: 'CONTRATO'})
        logger.debug(f"Coluna {coluna_contrato} renomeada para CONTRATO")
    
    # Contar linhas antes de remover duplicados
    total_linhas = len(df)
    
    # Remover duplicados na coluna CONTRATO (manter primeira ocorrência)
    df_processado = df.drop_duplicates(subset=['CONTRATO'], keep='first').copy()
    
    total_unicos = len(df_processado)
    total_duplicados = total_linhas - total_unicos
    
    logger.debug(f"3026-11 processado: total={total_linhas}, únicos={total_unicos}, duplicados={total_duplicados}")
    
    return df_processado, total_linhas, total_unicos, total_duplicados


def process_3026_15(df: pd.DataFrame, bank_name: str) -> tuple:
    """
    Processa arquivos 3026-15.
    - Formata coluna D como texto
    - Remove duplicados na coluna CONTRATO
    Retorna: (df_processado, total_linhas, total_unicos, total_duplicados)
    """
    logger.debug(f"Processando 3026-15 para {bank_name} - Linhas: {len(df)}, Colunas: {len(df.columns)}")
    logger.debug(f"Colunas do 3026-15: {list(df.columns)[:10]}...")
    
    # Verificar se DataFrame está vazio
    if df.empty:
        logger.warning(f"DataFrame 3026-15 está vazio para {bank_name}")
        return pd.DataFrame(), 0, 0, 0
    
    # Fazer cópia para evitar problemas de referência
    df = df.copy()
    
    # Formatar coluna D como texto
    df = format_column_d_as_text(df)
    
    # Verificar se tem coluna CONTRATO (tentar variações)
    coluna_contrato = None
    for nome in ['CONTRATO', 'CONTRATO_NUM', 'NUM_CONTRATO', 'NR_CONTRATO']:
        if nome in df.columns:
            coluna_contrato = nome
            break
    
    if coluna_contrato is None:
        logger.error(f"Coluna CONTRATO não encontrada. Colunas disponíveis: {list(df.columns)}")
        raise HTTPException(
            status_code=400,
            detail="Coluna 'CONTRATO' não encontrada no arquivo 3026-15"
        )
    
    # Renomear para CONTRATO se necessário
    if coluna_contrato != 'CONTRATO':
        df = df.rename(columns={coluna_contrato: 'CONTRATO'})
        logger.debug(f"Coluna {coluna_contrato} renomeada para CONTRATO")
    
    # Contar linhas antes de remover duplicados
    total_linhas = len(df)
    
    # Remover duplicados na coluna CONTRATO (manter primeira ocorrência)
    df_processado = df.drop_duplicates(subset=['CONTRATO'], keep='first').copy()
    
    total_unicos = len(df_processado)
    total_duplicados = total_linhas - total_unicos
    
    logger.debug(f"3026-15 processado: total={total_linhas}, únicos={total_unicos}, duplicados={total_duplicados}")
    
    return df_processado, total_linhas, total_unicos, total_duplicados


def process_3026_12(df: pd.DataFrame, bank_name: str) -> dict:
    """
    Processa arquivo 3026-12.
    - Coluna B: Manter apenas linhas onde = 52101
    - Coluna D: Formatar como texto
    - Colunas AA e AB: Deixar como número único (sem decimais)
    - Remove colunas BT e BU
    - Separa em AUD e NAUD, aplica filtros
    Retorna: {
        'aud': (df_aud, total_aud, unicos_aud, duplicados_aud),
        'naud': (df_naud, total_naud, unicos_naud, duplicados_naud)
    }
    """
    logger.debug(f"Processando 3026-12 para {bank_name} - Colunas: {len(df.columns)}, Linhas: {len(df)}")
    
    # Verificar se DataFrame está vazio
    if df.empty:
        logger.warning(f"DataFrame 3026-12 está vazio para {bank_name}")
        return {
            'aud': (pd.DataFrame(), 0, 0, 0),
            'naud': (pd.DataFrame(), 0, 0, 0)
        }
    
    # Fazer cópia para evitar problemas de referência
    df = df.copy()
    
    logger.debug(f"Colunas do 3026-12: {list(df.columns)[:15]}...")
    
    # 1. Coluna B - Manter apenas linhas onde = 52101
    if len(df.columns) > 1:
        coluna_b = df.columns[1]
        logger.debug(f"Coluna B (índice 1): {coluna_b}")
        # Converter para string, removendo .0 de números float
        df[coluna_b] = df[coluna_b].apply(
            lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x) if pd.notna(x) else ''
        )
        df[coluna_b] = df[coluna_b].str.strip()
        linhas_antes = len(df)
        
        # Verificar se existe o valor 52101
        valores_unicos = df[coluna_b].unique()[:10]
        logger.debug(f"Valores únicos na coluna B (amostra): {valores_unicos}")
        
        # Filtrar por 52101
        df_filtrado = df[df[coluna_b] == '52101'].copy()
        linhas_depois = len(df_filtrado)
        logger.debug(f"Filtro coluna B=52101: {linhas_antes} -> {linhas_depois} linhas")
        
        # Se não encontrou nenhuma linha, continuar sem o filtro
        if linhas_depois == 0:
            logger.warning(f"Nenhuma linha com valor 52101 encontrada. Continuando sem filtro da coluna B.")
        else:
            df = df_filtrado
    
    # Se não sobrou nenhuma linha
    if len(df) == 0:
        logger.warning("DataFrame vazio após processamento inicial")
        return {
            'aud': (pd.DataFrame(), 0, 0, 0),
            'naud': (pd.DataFrame(), 0, 0, 0)
        }
    
    # 2. Formatar coluna D como texto
    df = format_column_d_as_text(df)
    
    # 3. Colunas AA e AB - deixar como número único (sem decimais)
    for idx in [26, 27]:  # AA=26, AB=27 (0-based)
        if len(df.columns) > idx:
            col = df.columns[idx]
            logger.debug(f"Formatando coluna índice {idx} ({col}) como inteiro")
            df[col] = df[col].apply(
                lambda x: int(x) if pd.notna(x) and isinstance(x, (int, float)) else x
            )
    
    # 4. Remover colunas BT e BU (índices 71 e 72, 0-based)
    if len(df.columns) > 72:
        colunas_bt_bu = df.columns[71:73].tolist()
        logger.debug(f"Removendo colunas BT e BU: {colunas_bt_bu}")
        df = df.drop(columns=colunas_bt_bu, errors='ignore')
    
    # Verificar coluna AUDITADO (tentar diferentes nomes)
    coluna_auditado = None
    for nome in ['AUDITADO', 'AUD', 'AUDIT']:
        if nome in df.columns:
            coluna_auditado = nome
            break
    
    if coluna_auditado is None:
        logger.error(f"Coluna AUDITADO não encontrada. Colunas disponíveis: {list(df.columns)}")
        raise HTTPException(
            status_code=400,
            detail=f"Coluna 'AUDITADO' não encontrada no arquivo 3026-12"
        )
    
    # Verificar coluna CONTRATO (tentar variações)
    coluna_contrato = None
    for nome in ['CONTRATO', 'CONTRATO_NUM', 'NUM_CONTRATO', 'NR_CONTRATO']:
        if nome in df.columns:
            coluna_contrato = nome
            break
    
    if coluna_contrato is None:
        logger.error(f"Coluna CONTRATO não encontrada. Colunas disponíveis: {list(df.columns)}")
        raise HTTPException(
            status_code=400,
            detail=f"Coluna 'CONTRATO' não encontrada no arquivo 3026-12"
        )
    
    # Renomear para CONTRATO se necessário
    if coluna_contrato != 'CONTRATO':
        df = df.rename(columns={coluna_contrato: 'CONTRATO'})
        logger.debug(f"Coluna {coluna_contrato} renomeada para CONTRATO")
    
    # Verificar colunas de filtro (nomes reais das colunas)
    filter_cols = ['DEST.PAGAM', 'DEST.COMPLEM']
    has_filter_cols = any(col in df.columns for col in filter_cols)
    
    # Converter AUDITADO para string e normalizar
    df[coluna_auditado] = df[coluna_auditado].astype(str).str.upper().str.strip()
    
    # Verificar valores únicos de AUDITADO
    valores_auditado = df[coluna_auditado].unique()
    logger.debug(f"Valores únicos de AUDITADO: {valores_auditado}")
    
    # Separar em AUD e NAUD (aceitar variações)
    df_aud = df[df[coluna_auditado].isin(['AUDI', 'AUD', 'AUDITADO'])].copy()
    df_naud = df[df[coluna_auditado].isin(['NAUD', 'NAO AUDITADO', 'NAUDITADO', 'NÃO AUDITADO'])].copy()
    
    logger.debug(f"Após separação: AUD={len(df_aud)}, NAUD={len(df_naud)}")
    
    # Aplicar filtros se as colunas existirem
    valores_filtro = ['0X0', '1X4', '6X4', '8X4', '0x0', '1x4', '6x4', '8x4']
    
    if has_filter_cols:
        for col in filter_cols:
            if col in df_aud.columns and len(df_aud) > 0:
                df_aud[col] = df_aud[col].astype(str).str.upper().str.strip()
                mask = ~df_aud[col].isin(valores_filtro)
                df_aud = df_aud[mask].copy()
            
            if col in df_naud.columns and len(df_naud) > 0:
                df_naud[col] = df_naud[col].astype(str).str.upper().str.strip()
                mask = ~df_naud[col].isin(valores_filtro)
                df_naud = df_naud[mask].copy()
    
    # Processar AUD
    total_aud = len(df_aud)
    if total_aud > 0:
        df_aud_unicos = df_aud.drop_duplicates(subset=['CONTRATO'], keep='first')
        unicos_aud = len(df_aud_unicos)
    else:
        df_aud_unicos = pd.DataFrame()
        unicos_aud = 0
    duplicados_aud = total_aud - unicos_aud
    
    # Processar NAUD
    total_naud = len(df_naud)
    if total_naud > 0:
        df_naud_unicos = df_naud.drop_duplicates(subset=['CONTRATO'], keep='first')
        unicos_naud = len(df_naud_unicos)
    else:
        df_naud_unicos = pd.DataFrame()
        unicos_naud = 0
    duplicados_naud = total_naud - unicos_naud
    
    logger.debug(f"AUD: total={total_aud}, únicos={unicos_aud}, duplicados={duplicados_aud}")
    logger.debug(f"NAUD: total={total_naud}, únicos={unicos_naud}, duplicados={duplicados_naud}")
    
    return {
        'aud': (df_aud_unicos, total_aud, unicos_aud, duplicados_aud),
        'naud': (df_naud_unicos, total_naud, unicos_naud, duplicados_naud)
    }


def apply_excel_formatting(writer, df: pd.DataFrame, sheet_name: str):
    """
    Aplica formatação no Excel:
    - Colunas de data: formato DD/MM/YYYY
    - Coluna CONTRATO: formato texto
    """
    worksheet = writer.sheets[sheet_name]
    
    # Lista de possíveis nomes de colunas de data
    colunas_data_conhecidas = [
        'DT.ASS.', 'DT.EVENTO', 'DT.HAB.', 'DT.PROC.HAB.',
        'DT.ASS', 'DT.EVENTO', 'DT.HAB', 'DT.PROC.HAB',
        'DATA ASS.', 'DATA EVENTO', 'DATA HAB.', 'DATA PROC.HAB.',
        'DT.BASE', 'DT.TERM.ANALISE', 'DT.MANIFESTACAO', 'DT.POS.NOVACAO',
        'DT.ULT.AUDITORIA', 'DT.ULT.NEGOCIACAO', 'DATA STATUS'
    ]
    
    # Formatar TODAS as colunas de data (detectar por nome)
    for col_name in df.columns:
        is_date_col = (
            col_name in colunas_data_conhecidas or 
            col_name.upper().startswith('DT.') or 
            col_name.upper().startswith('DATA')
        )
        
        if is_date_col:
            try:
                col_idx = df.columns.get_loc(col_name) + 1
                col_letter = get_column_letter(col_idx)
                for row in range(2, len(df) + 2):
                    cell = worksheet[f"{col_letter}{row}"]
                    cell.number_format = 'DD/MM/YYYY'
            except Exception as e:
                logger.warning(f"Erro ao formatar coluna de data {col_name}: {e}")
    
    # Formatar coluna CONTRATO como texto
    if 'CONTRATO' in df.columns:
        col_idx = df.columns.get_loc('CONTRATO') + 1
        col_letter = get_column_letter(col_idx)
        for row in range(2, len(df) + 2):
            cell = worksheet[f"{col_letter}{row}"]
            cell.number_format = '@'
    
    # Formatar coluna D (índice 3) como texto se existir
    if len(df.columns) > 3:
        col_idx = 4  # Coluna D (1-based)
        col_letter = get_column_letter(col_idx)
        for row in range(2, len(df) + 2):
            cell = worksheet[f"{col_letter}{row}"]
            cell.number_format = '@'


def add_column_ae_sum(writer, df: pd.DataFrame, sheet_name: str):
    """
    Adiciona soma da coluna AE na segunda linha abaixo do último valor.
    Coluna AE = índice 30 (0-based) ou 31 (1-based em Excel)
    """
    if len(df.columns) <= 30 or len(df) == 0:
        logger.debug(f"Não há coluna AE ou dados para adicionar soma em {sheet_name}")
        return
    
    try:
        worksheet = writer.sheets[sheet_name]
        
        # Coluna AE = índice 30 (0-based), que é coluna 31 no Excel (letra AE)
        col_ae = df.columns[30]
        col_letter = get_column_letter(31)  # AE
        
        # Linha onde colocar a soma (2 linhas abaixo do último dado)
        # Dados começam na linha 2, então última linha de dados = len(df) + 1
        # Soma vai 2 linhas abaixo = len(df) + 3
        row_sum = len(df) + 3
        
        # Calcular soma
        try:
            # Tentar converter coluna para numérico e somar
            valores_numericos = pd.to_numeric(df[col_ae], errors='coerce')
            soma = valores_numericos.sum()
            
            if pd.notna(soma):
                # Adicionar rótulo
                worksheet[f"AD{row_sum}"] = "SOMA AE:"
                # Adicionar valor da soma
                worksheet[f"{col_letter}{row_sum}"] = soma
                worksheet[f"{col_letter}{row_sum}"].number_format = '#,##0.00'
                
                logger.debug(f"Soma da coluna AE adicionada em {sheet_name}: {soma}")
        except Exception as e:
            logger.warning(f"Erro ao calcular soma da coluna AE: {e}")
            
    except Exception as e:
        logger.warning(f"Erro ao adicionar soma da coluna AE em {sheet_name}: {e}")


def save_processed_file(df: pd.DataFrame, filepath: str):
    """Salva arquivo Excel processado com formatação"""
    try:
        filepath_obj = Path(filepath)
        filepath_obj.parent.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"Salvando arquivo: {filepath}")
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Dados', index=False)
            apply_excel_formatting(writer, df, 'Dados')
        
        if not filepath_obj.exists():
            raise Exception(f"Arquivo não foi salvo: {filepath}")
        
        logger.debug(f"Arquivo salvo com sucesso: {filepath}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo {filepath}: {e}")
        raise


async def process_contratos(
    files: List[UploadFile], 
    bank_type: str, 
    filter_type: str = "todos", 
    file_type: str = "todos",
    period_filter: str = "todos"
) -> StreamingResponse:
    """
    Processa múltiplas planilhas Excel de contratos.
    
    Args:
        files: Lista de arquivos Excel
        bank_type: "bemge" ou "minas_caixa"
        filter_type: "auditado", "nauditado" ou "todos"
        file_type: "3026-11", "3026-12", "3026-15" ou "todos"
        period_filter: "todos" ou "ultimos_2_meses"
    
    Returns:
        StreamingResponse com arquivo Excel consolidado
    """
    try:
        logger.info(f"Iniciando processamento: bank_type={bank_type}, filter_type={filter_type}, file_type={file_type}, period_filter={period_filter}")
        logger.info(f"Arquivos recebidos: {[f.filename for f in files]}")
        
        # Validar bank_type
        bank_type_normalized = bank_type.lower().replace(" ", "_")
        if bank_type_normalized not in ['bemge', 'minas_caixa']:
            raise HTTPException(
                status_code=400,
                detail="bank_type deve ser 'bemge' ou 'minas_caixa'"
            )
        
        # Validar filter_type
        if filter_type not in ['auditado', 'nauditado', 'todos']:
            raise HTTPException(
                status_code=400,
                detail="filter_type deve ser 'auditado', 'nauditado' ou 'todos'"
            )
        
        # Validar file_type
        if file_type not in ['3026-11', '3026-12', '3026-15', 'todos']:
            raise HTTPException(
                status_code=400,
                detail="file_type deve ser '3026-11', '3026-12', '3026-15' ou 'todos'"
            )
        
        # Validar period_filter
        if period_filter not in ['todos', 'ultimos_2_meses']:
            raise HTTPException(
                status_code=400,
                detail="period_filter deve ser 'todos' ou 'ultimos_2_meses'"
            )
        
        # Validar se pelo menos um arquivo foi enviado
        if not files or len(files) == 0:
            raise HTTPException(
                status_code=400,
                detail="Pelo menos um arquivo deve ser enviado"
            )
        
        bank_name = get_bank_name(bank_type_normalized)
        sheet_names = get_sheet_names(bank_type_normalized)
        
        # Usar underscore em vez de espaços para nomes de pastas
        bank_folder = bank_type_normalized.replace(" ", "_")
        base_dir = Path(f"arquivo_morto/{bank_folder}")
        filtragem_dir = Path("arquivo_morto/3026 - Filtragens")
        
        # Criar estrutura de pastas
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            filtragem_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Pastas criadas: {base_dir}, {filtragem_dir}")
        except Exception as e:
            logger.error(f"Erro ao criar pastas: {e}")
            raise HTTPException(status_code=500, detail=f"Erro ao criar pastas: {str(e)}")
        
        # Estruturas para consolidar dados
        all_contratos = []
        contratos_repetidos = []
        resumo_geral = []
        contratos_por_banco = []
        
        # Estruturas para separar por abas
        dados_por_aba = {
            '3026-11': [],
            '3026-12 AUD': [],
            '3026-12 NAUD': [],
            '3026-15': []
        }
        
        # Estrutura para contratos dos últimos 2 meses
        dados_ultimos_2_meses = []
        
        # Processar cada arquivo
        for file in files:
            filename = file.filename
            filename_upper = filename.upper()
            
            logger.debug(f"Processando arquivo: {filename}")
            
            # Filtrar por tipo de arquivo se especificado
            if file_type != "todos":
                file_type_normalized = file_type.upper().replace("-", "")
                if file_type.upper() not in filename_upper and file_type_normalized not in filename_upper:
                    logger.debug(f"Arquivo {filename} ignorado (não é {file_type})")
                    continue
            
            # Ler arquivo
            try:
                contents = await file.read()
                df = pd.read_excel(io.BytesIO(contents), engine='openpyxl')
                logger.debug(f"Arquivo lido: {len(df)} linhas, {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao ler arquivo {filename}: {e}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Erro ao ler arquivo {filename}: {str(e)}"
                )
            
            # Formatação de datas
            df = format_date_columns(df)
            
            # Remover colunas gerais
            df = remove_general_columns(df)
            
            # Formatação da coluna CONTRATO
            df = format_contrato_column(df)
            
            # Detectar tipo de arquivo
            detected_file_type = detect_file_type(filename)
            
            if detected_file_type == '3026-11':
                df_processado, total_linhas, total_unicos, total_duplicados = process_3026_11(df, bank_name)
                
                if df_processado.empty:
                    continue
                
                df_processado['TIPO_ARQUIVO'] = '3026-11'
                df_processado['BANCO'] = bank_name
                df_processado['DUPLICADO'] = df_processado['CONTRATO'].duplicated(keep=False)
                
                # Aplicar filtro de auditado se necessário
                if filter_type != 'todos' and 'AUDITADO' in df_processado.columns:
                    df_processado['AUDITADO'] = df_processado['AUDITADO'].astype(str).str.upper().str.strip()
                    if filter_type == 'auditado':
                        df_processado = df_processado[df_processado['AUDITADO'].isin(['AUDI', 'AUD', 'AUDITADO'])].copy()
                    elif filter_type == 'nauditado':
                        df_processado = df_processado[df_processado['AUDITADO'].isin(['NAUD', 'NAO AUDITADO', 'NAUDITADO'])].copy()
                
                # Aplicar filtro de período se necessário
                if period_filter == 'ultimos_2_meses':
                    df_processado = filter_last_2_months(df_processado)
                
                all_contratos.append(df_processado)
                dados_por_aba['3026-11'].append(df_processado.copy())
                
                # Adicionar aos últimos 2 meses
                df_2_meses = filter_last_2_months(df_processado)
                if not df_2_meses.empty:
                    dados_ultimos_2_meses.append(df_2_meses.copy())
                
                # Contratos repetidos
                df_repetidos = df_processado[df_processado['DUPLICADO'] == True].copy()
                if len(df_repetidos) > 0:
                    contratos_repetidos.append(df_repetidos)
                
                # Salvar arquivo
                save_filename = f"3026-11 - {bank_name} - {total_unicos} (CONTRATOS).xlsx"
                save_filepath = base_dir / save_filename
                save_processed_file(df_processado, str(save_filepath))
                
                if 'AUDITADO' in df_processado.columns:
                    filepath_filtragem = filtragem_dir / save_filename
                    save_processed_file(df_processado, str(filepath_filtragem))
                
                resumo_geral.append({
                    'ARQUIVO': filename,
                    'TIPO': '3026-11',
                    'TOTAL_LINHAS': total_linhas,
                    'CONTRATOS_UNICOS': total_unicos,
                    'CONTRATOS_DUPLICADOS': total_duplicados,
                    'BANCO': bank_name
                })
                
                df_banco = df_processado[['CONTRATO', 'TIPO_ARQUIVO', 'BANCO']].copy()
                contratos_por_banco.append(df_banco)
            
            elif detected_file_type == '3026-15':
                df_processado, total_linhas, total_unicos, total_duplicados = process_3026_15(df, bank_name)
                
                if df_processado.empty:
                    continue
                
                df_processado['TIPO_ARQUIVO'] = '3026-15'
                df_processado['BANCO'] = bank_name
                df_processado['DUPLICADO'] = df_processado['CONTRATO'].duplicated(keep=False)
                
                # Aplicar filtro de auditado se necessário
                if filter_type != 'todos' and 'AUDITADO' in df_processado.columns:
                    df_processado['AUDITADO'] = df_processado['AUDITADO'].astype(str).str.upper().str.strip()
                    if filter_type == 'auditado':
                        df_processado = df_processado[df_processado['AUDITADO'].isin(['AUDI', 'AUD', 'AUDITADO'])].copy()
                    elif filter_type == 'nauditado':
                        df_processado = df_processado[df_processado['AUDITADO'].isin(['NAUD', 'NAO AUDITADO', 'NAUDITADO'])].copy()
                
                # Aplicar filtro de período se necessário
                if period_filter == 'ultimos_2_meses':
                    df_processado = filter_last_2_months(df_processado)
                
                all_contratos.append(df_processado)
                dados_por_aba['3026-15'].append(df_processado.copy())
                
                # Adicionar aos últimos 2 meses
                df_2_meses = filter_last_2_months(df_processado)
                if not df_2_meses.empty:
                    dados_ultimos_2_meses.append(df_2_meses.copy())
                
                # Contratos repetidos
                df_repetidos = df_processado[df_processado['DUPLICADO'] == True].copy()
                if len(df_repetidos) > 0:
                    contratos_repetidos.append(df_repetidos)
                
                # Salvar arquivo
                save_filename = f"3026-15 - {bank_name} - {total_unicos} (CONTRATOS).xlsx"
                save_filepath = base_dir / save_filename
                save_processed_file(df_processado, str(save_filepath))
                
                if 'AUDITADO' in df_processado.columns:
                    filepath_filtragem = filtragem_dir / save_filename
                    save_processed_file(df_processado, str(filepath_filtragem))
                
                resumo_geral.append({
                    'ARQUIVO': filename,
                    'TIPO': '3026-15',
                    'TOTAL_LINHAS': total_linhas,
                    'CONTRATOS_UNICOS': total_unicos,
                    'CONTRATOS_DUPLICADOS': total_duplicados,
                    'BANCO': bank_name
                })
                
                df_banco = df_processado[['CONTRATO', 'TIPO_ARQUIVO', 'BANCO']].copy()
                contratos_por_banco.append(df_banco)
            
            elif detected_file_type == '3026-12':
                resultados = process_3026_12(df, bank_name)
                
                # Determinar quais tipos processar baseado no filter_type
                tipos_processar = []
                if filter_type == 'auditado':
                    tipos_processar = ['aud']
                elif filter_type == 'nauditado':
                    tipos_processar = ['naud']
                else:
                    tipos_processar = ['aud', 'naud']
                
                for tipo_aud in tipos_processar:
                    df_processado, total_linhas, total_unicos, total_duplicados = resultados[tipo_aud]
                    
                    if len(df_processado) == 0:
                        logger.warning(f"Nenhum dado para 3026-12 {tipo_aud.upper()}")
                        continue
                    
                    df_processado = df_processado.copy()
                    df_processado['TIPO_ARQUIVO'] = '3026-12'
                    df_processado['AUDITADO_TIPO'] = tipo_aud.upper()
                    df_processado['BANCO'] = bank_name
                    df_processado['DUPLICADO'] = df_processado['CONTRATO'].duplicated(keep=False)
                    
                    # Aplicar filtro de período se necessário
                    if period_filter == 'ultimos_2_meses':
                        df_processado = filter_last_2_months(df_processado)
                    
                    all_contratos.append(df_processado)
                    
                    aba_nome = f"3026-12 {tipo_aud.upper()}"
                    if aba_nome in dados_por_aba:
                        dados_por_aba[aba_nome].append(df_processado.copy())
                    
                    # Adicionar aos últimos 2 meses
                    df_2_meses = filter_last_2_months(df_processado)
                    if not df_2_meses.empty:
                        dados_ultimos_2_meses.append(df_2_meses.copy())
                    
                    # Contratos repetidos
                    df_repetidos = df_processado[df_processado['DUPLICADO'] == True].copy()
                    if len(df_repetidos) > 0:
                        contratos_repetidos.append(df_repetidos)
                    
                    # Salvar arquivo
                    tipo_nome = 'AUD' if tipo_aud == 'aud' else 'NAUD'
                    save_filename = f"3026-12 - {bank_name} - {tipo_nome} - {total_unicos} (CONTRATOS).xlsx"
                    save_filepath = base_dir / save_filename
                    save_processed_file(df_processado, str(save_filepath))
                    
                    filepath_filtragem = filtragem_dir / save_filename
                    save_processed_file(df_processado, str(filepath_filtragem))
                    
                    resumo_geral.append({
                        'ARQUIVO': filename,
                        'TIPO': f'3026-12-{tipo_nome}',
                        'TOTAL_LINHAS': total_linhas,
                        'CONTRATOS_UNICOS': total_unicos,
                        'CONTRATOS_DUPLICADOS': total_duplicados,
                        'BANCO': bank_name
                    })
                    
                    df_banco = df_processado[['CONTRATO', 'TIPO_ARQUIVO', 'BANCO', 'AUDITADO_TIPO']].copy()
                    contratos_por_banco.append(df_banco)
        
        # Consolidar todos os dados
        if not all_contratos:
            raise HTTPException(
                status_code=400,
                detail="Nenhum arquivo válido foi processado"
            )
        
        df_all_contratos = pd.concat(all_contratos, ignore_index=True)
        
        logger.info(f"Total de contratos consolidados: {len(df_all_contratos)}")
        
        # Aplicar filtro final baseado em filter_type
        if filter_type != 'todos':
            if 'AUDITADO_TIPO' in df_all_contratos.columns:
                if filter_type == 'auditado':
                    df_all_contratos = df_all_contratos[df_all_contratos['AUDITADO_TIPO'] == 'AUD'].copy()
                elif filter_type == 'nauditado':
                    df_all_contratos = df_all_contratos[df_all_contratos['AUDITADO_TIPO'] == 'NAUD'].copy()
            elif 'AUDITADO' in df_all_contratos.columns:
                df_all_contratos['AUDITADO'] = df_all_contratos['AUDITADO'].astype(str).str.upper().str.strip()
                if filter_type == 'auditado':
                    df_all_contratos = df_all_contratos[df_all_contratos['AUDITADO'].isin(['AUDI', 'AUD', 'AUDITADO'])].copy()
                elif filter_type == 'nauditado':
                    df_all_contratos = df_all_contratos[df_all_contratos['AUDITADO'].isin(['NAUD', 'NAO AUDITADO', 'NAUDITADO'])].copy()
        
        # Criar DataFrame de contratos repetidos
        if contratos_repetidos:
            df_repetidos = pd.concat(contratos_repetidos, ignore_index=True)
            if filter_type != 'todos':
                if 'AUDITADO_TIPO' in df_repetidos.columns:
                    if filter_type == 'auditado':
                        df_repetidos = df_repetidos[df_repetidos['AUDITADO_TIPO'] == 'AUD'].copy()
                    elif filter_type == 'nauditado':
                        df_repetidos = df_repetidos[df_repetidos['AUDITADO_TIPO'] == 'NAUD'].copy()
                elif 'AUDITADO' in df_repetidos.columns:
                    df_repetidos['AUDITADO'] = df_repetidos['AUDITADO'].astype(str).str.upper().str.strip()
                    if filter_type == 'auditado':
                        df_repetidos = df_repetidos[df_repetidos['AUDITADO'].isin(['AUDI', 'AUD', 'AUDITADO'])].copy()
                    elif filter_type == 'nauditado':
                        df_repetidos = df_repetidos[df_repetidos['AUDITADO'].isin(['NAUD', 'NAO AUDITADO', 'NAUDITADO'])].copy()
        else:
            df_repetidos = pd.DataFrame()
        
        # Criar DataFrame de últimos 2 meses
        if dados_ultimos_2_meses:
            df_ultimos_2_meses = pd.concat(dados_ultimos_2_meses, ignore_index=True)
        else:
            df_ultimos_2_meses = pd.DataFrame()
        
        # Criar DataFrame de resumo geral
        df_resumo = pd.DataFrame(resumo_geral)
        
        # Calcular totais
        total_geral = len(df_all_contratos)
        
        if 'AUDITADO_TIPO' in df_all_contratos.columns:
            total_auditados = len(df_all_contratos[df_all_contratos['AUDITADO_TIPO'] == 'AUD'])
            total_nauditados = len(df_all_contratos[df_all_contratos['AUDITADO_TIPO'] == 'NAUD'])
        elif 'AUDITADO' in df_all_contratos.columns:
            df_all_contratos['AUDITADO'] = df_all_contratos['AUDITADO'].astype(str).str.upper().str.strip()
            total_auditados = len(df_all_contratos[df_all_contratos['AUDITADO'].isin(['AUDI', 'AUD', 'AUDITADO'])])
            total_nauditados = len(df_all_contratos[df_all_contratos['AUDITADO'].isin(['NAUD', 'NAO AUDITADO', 'NAUDITADO'])])
        else:
            total_auditados = 0
            total_nauditados = 0
        
        total_repetidos = len(df_repetidos) if not df_repetidos.empty else 0
        
        linha_totais = pd.DataFrame([{
            'ARQUIVO': 'TOTAL GERAL',
            'TIPO': '-',
            'TOTAL_LINHAS': total_geral,
            'CONTRATOS_UNICOS': total_geral - total_repetidos,
            'CONTRATOS_DUPLICADOS': total_repetidos,
            'BANCO': bank_name
        }])
        df_resumo = pd.concat([df_resumo, linha_totais], ignore_index=True)
        
        # Criar arquivo Excel consolidado
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Aba: Resumo Geral
            df_resumo.to_excel(writer, sheet_name='Resumo Geral', index=False)
            
            # Aba: 3026-11 com nome padronizado
            if dados_por_aba['3026-11']:
                df_3026_11 = pd.concat(dados_por_aba['3026-11'], ignore_index=True)
                nome_aba_11 = sheet_names['3026-11'][:31]  # Limitar a 31 caracteres
                df_3026_11.to_excel(writer, sheet_name=nome_aba_11, index=False)
                apply_excel_formatting(writer, df_3026_11, nome_aba_11)
            
            # Aba: 3026-12 AUD com nome padronizado
            if dados_por_aba['3026-12 AUD']:
                df_3026_12_aud = pd.concat(dados_por_aba['3026-12 AUD'], ignore_index=True)
                nome_aba_12_aud = sheet_names['3026-12-AUD'][:31]
                df_3026_12_aud.to_excel(writer, sheet_name=nome_aba_12_aud, index=False)
                apply_excel_formatting(writer, df_3026_12_aud, nome_aba_12_aud)
                # Adicionar soma da coluna AE
                add_column_ae_sum(writer, df_3026_12_aud, nome_aba_12_aud)
            
            # Aba: 3026-12 NAUD com nome padronizado
            if dados_por_aba['3026-12 NAUD']:
                df_3026_12_naud = pd.concat(dados_por_aba['3026-12 NAUD'], ignore_index=True)
                nome_aba_12_naud = sheet_names['3026-12-NAUD'][:31]
                df_3026_12_naud.to_excel(writer, sheet_name=nome_aba_12_naud, index=False)
                apply_excel_formatting(writer, df_3026_12_naud, nome_aba_12_naud)
                # Adicionar soma da coluna AE
                add_column_ae_sum(writer, df_3026_12_naud, nome_aba_12_naud)
            
            # Aba: 3026-15 com nome padronizado
            if dados_por_aba['3026-15']:
                df_3026_15 = pd.concat(dados_por_aba['3026-15'], ignore_index=True)
                nome_aba_15 = sheet_names['3026-15'][:31]
                df_3026_15.to_excel(writer, sheet_name=nome_aba_15, index=False)
                apply_excel_formatting(writer, df_3026_15, nome_aba_15)
            
            # Aba: Últimos 2 Meses
            if not df_ultimos_2_meses.empty:
                df_ultimos_2_meses.to_excel(writer, sheet_name='Últimos 2 Meses', index=False)
                apply_excel_formatting(writer, df_ultimos_2_meses, 'Últimos 2 Meses')
            else:
                pd.DataFrame({'Mensagem': ['Nenhum contrato encontrado nos últimos 2 meses']}).to_excel(
                    writer, sheet_name='Últimos 2 Meses', index=False
                )
            
            # Aba: Todos os Contratos
            if not df_all_contratos.empty:
                df_all_contratos.to_excel(writer, sheet_name='Todos Contratos', index=False)
                apply_excel_formatting(writer, df_all_contratos, 'Todos Contratos')
            
            # Aba: Contratos Repetidos
            if not df_repetidos.empty:
                df_repetidos.to_excel(writer, sheet_name='Repetidos', index=False)
                apply_excel_formatting(writer, df_repetidos, 'Repetidos')
            else:
                pd.DataFrame({'Mensagem': ['Nenhum contrato repetido encontrado']}).to_excel(
                    writer, sheet_name='Repetidos', index=False
                )
        
        # Resetar ponteiro e ler dados
        output.seek(0)
        excel_data = output.read()
        output.close()
        
        # Nome do arquivo de saída
        filtro_nome = filter_type.upper()
        banco_nome = "BEMGE" if bank_type_normalized == "bemge" else "MINAS_CAIXA"
        tipo_nome = f"_{file_type}" if file_type != "todos" else ""
        periodo_nome = "_2MESES" if period_filter == "ultimos_2_meses" else ""
        
        filename_output = f"contratos{tipo_nome}_{banco_nome}_{filtro_nome}{periodo_nome}_consolidado.xlsx"
        
        logger.info(f"Processamento concluído. Arquivo: {filename_output}")
        
        return StreamingResponse(
            io.BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename_output}"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao processar contratos: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar contratos: {str(e)}"
        )
