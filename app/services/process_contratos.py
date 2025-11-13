import pandas as pd
import io
import os
import re
from typing import List
from fastapi import HTTPException, UploadFile
from fastapi.responses import StreamingResponse


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


def process_3026_11_15(df: pd.DataFrame, bank_name: str, file_type: str) -> tuple:
    """
    Processa arquivos 3026-11 e 3026-15.
    Retorna: (df_processado, total_linhas, total_unicos)
    """
    # Verificar se tem coluna CONTRATO
    if 'CONTRATO' not in df.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Coluna 'CONTRATO' não encontrada no arquivo {file_type}"
        )
    
    # Contar linhas antes de remover duplicados
    total_linhas = len(df)
    
    # Remover duplicados na coluna CONTRATO (manter primeira ocorrência)
    df_processado = df.drop_duplicates(subset=['CONTRATO'], keep='first').copy()
    
    total_unicos = len(df_processado)
    total_duplicados = total_linhas - total_unicos
    
    return df_processado, total_linhas, total_unicos, total_duplicados


def process_3026_12(df: pd.DataFrame, bank_name: str) -> dict:
    """
    Processa arquivo 3026-12.
    Separa em AUD e NAUD, aplica filtros.
    Retorna: {
        'aud': (df_aud, total_aud, unicos_aud, duplicados_aud),
        'naud': (df_naud, total_naud, unicos_naud, duplicados_naud)
    }
    """
    # Verificar colunas necessárias
    required_cols = ['AUDITADO', 'CONTRATO']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise HTTPException(
            status_code=400,
            detail=f"Colunas não encontradas no arquivo 3026-12: {', '.join(missing_cols)}"
        )
    
    # Verificar colunas de filtro
    filter_cols = ['DESTINO DE PAGAMENTO', 'DESTINO DE COMPLEMENTO']
    has_filter_cols = any(col in df.columns for col in filter_cols)
    
    # Converter AUDITADO para string e normalizar
    df['AUDITADO'] = df['AUDITADO'].astype(str).str.upper().str.strip()
    
    # Separar em AUD e NAUD
    df_aud = df[df['AUDITADO'] == 'AUDI'].copy()
    df_naud = df[df['AUDITADO'] == 'NAUD'].copy()
    
    # Aplicar filtros se as colunas existirem
    valores_filtro = ['0x0', '1x4', '6x4', '8x4']
    
    if has_filter_cols:
        for col in filter_cols:
            if col in df_aud.columns:
                # Converter para string e normalizar
                df_aud[col] = df_aud[col].astype(str).str.upper().str.strip()
                # Remover linhas com valores filtrados
                mask = ~df_aud[col].isin(valores_filtro)
                df_aud = df_aud[mask].copy()
            
            if col in df_naud.columns:
                # Converter para string e normalizar
                df_naud[col] = df_naud[col].astype(str).str.upper().str.strip()
                # Remover linhas com valores filtrados
                mask = ~df_naud[col].isin(valores_filtro)
                df_naud = df_naud[mask].copy()
    
    # Processar AUD
    total_aud = len(df_aud)
    df_aud_unicos = df_aud.drop_duplicates(subset=['CONTRATO'], keep='first')
    unicos_aud = len(df_aud_unicos)
    duplicados_aud = total_aud - unicos_aud
    
    # Processar NAUD
    total_naud = len(df_naud)
    df_naud_unicos = df_naud.drop_duplicates(subset=['CONTRATO'], keep='first')
    unicos_naud = len(df_naud_unicos)
    duplicados_naud = total_naud - unicos_naud
    
    return {
        'aud': (df_aud_unicos, total_aud, unicos_aud, duplicados_aud),
        'naud': (df_naud_unicos, total_naud, unicos_naud, duplicados_naud)
    }


def save_processed_file(df: pd.DataFrame, filepath: str):
    """Salva arquivo Excel processado"""
    # Criar diretório se não existir
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Salvar arquivo
    df.to_excel(filepath, index=False, engine='openpyxl')


async def process_contratos(files: List[UploadFile], bank_type: str) -> StreamingResponse:
    """
    Processa múltiplas planilhas Excel de contratos.
    
    Args:
        files: Lista de arquivos Excel
        bank_type: "bemge" ou "minas_caixa"
    
    Returns:
        StreamingResponse com arquivo Excel consolidado
    """
    try:
        # Validar bank_type
        if bank_type not in ['bemge', 'minas_caixa']:
            raise HTTPException(
                status_code=400,
                detail="bank_type deve ser 'bemge' ou 'minas_caixa'"
            )
        
        bank_name = get_bank_name(bank_type)
        base_dir = f"arquivo_morto/{bank_type}"
        
        # Estruturas para consolidar dados
        all_contratos = []
        contratos_repetidos = []
        resumo_geral = []
        contratos_por_banco = []
        
        # Processar cada arquivo
        for file in files:
            # Ler arquivo
            contents = await file.read()
            df = pd.read_excel(io.BytesIO(contents), engine='openpyxl')
            
            # Detectar tipo de arquivo
            file_type = detect_file_type(file.filename)
            
            if file_type in ['3026-11', '3026-15']:
                # Processar 3026-11 ou 3026-15
                df_processado, total_linhas, total_unicos, total_duplicados = process_3026_11_15(
                    df, bank_name, file_type
                )
                
                # Adicionar coluna de tipo
                df_processado['TIPO_ARQUIVO'] = file_type
                df_processado['BANCO'] = bank_name
                
                # Identificar duplicados
                df_processado['DUPLICADO'] = df_processado['CONTRATO'].duplicated(keep=False)
                
                # Adicionar aos consolidados
                all_contratos.append(df_processado)
                
                # Contratos repetidos
                df_repetidos = df_processado[df_processado['DUPLICADO'] == True].copy()
                if len(df_repetidos) > 0:
                    contratos_repetidos.append(df_repetidos)
                
                # Salvar arquivo processado
                filename = f"{file_type} - {bank_name} - {total_unicos} (CONTRATOS).xlsx"
                filepath = os.path.join(base_dir, filename)
                save_processed_file(df_processado, filepath)
                
                # Adicionar ao resumo
                resumo_geral.append({
                    'ARQUIVO': file.filename,
                    'TIPO': file_type,
                    'TOTAL_LINHAS': total_linhas,
                    'CONTRATOS_UNICOS': total_unicos,
                    'CONTRATOS_DUPLICADOS': total_duplicados,
                    'BANCO': bank_name
                })
                
                # Adicionar aos contratos por banco
                df_banco = df_processado[['CONTRATO', 'TIPO_ARQUIVO', 'BANCO']].copy()
                contratos_por_banco.append(df_banco)
            
            elif file_type == '3026-12':
                # Processar 3026-12
                resultados = process_3026_12(df, bank_name)
                
                for tipo_aud in ['aud', 'naud']:
                    df_processado, total_linhas, total_unicos, total_duplicados = resultados[tipo_aud]
                    
                    # Adicionar colunas
                    df_processado['TIPO_ARQUIVO'] = '3026-12'
                    df_processado['AUDITADO_TIPO'] = tipo_aud.upper()
                    df_processado['BANCO'] = bank_name
                    df_processado['DUPLICADO'] = df_processado['CONTRATO'].duplicated(keep=False)
                    
                    # Adicionar aos consolidados
                    all_contratos.append(df_processado)
                    
                    # Contratos repetidos
                    df_repetidos = df_processado[df_processado['DUPLICADO'] == True].copy()
                    if len(df_repetidos) > 0:
                        contratos_repetidos.append(df_repetidos)
                    
                    # Salvar arquivo processado
                    tipo_nome = 'AUD' if tipo_aud == 'aud' else 'NAUD'
                    filename = f"3026-12 - {bank_name} - {tipo_nome} - {total_unicos} (CONTRATOS).xlsx"
                    filepath = os.path.join(base_dir, filename)
                    save_processed_file(df_processado, filepath)
                    
                    # Adicionar ao resumo
                    resumo_geral.append({
                        'ARQUIVO': file.filename,
                        'TIPO': f'3026-12-{tipo_nome}',
                        'TOTAL_LINHAS': total_linhas,
                        'CONTRATOS_UNICOS': total_unicos,
                        'CONTRATOS_DUPLICADOS': total_duplicados,
                        'BANCO': bank_name
                    })
                    
                    # Adicionar aos contratos por banco
                    df_banco = df_processado[['CONTRATO', 'TIPO_ARQUIVO', 'BANCO', 'AUDITADO_TIPO']].copy()
                    contratos_por_banco.append(df_banco)
        
        # Consolidar todos os dados
        if not all_contratos:
            raise HTTPException(
                status_code=400,
                detail="Nenhum arquivo válido foi processado"
            )
        
        df_all_contratos = pd.concat(all_contratos, ignore_index=True)
        
        # Criar DataFrame de contratos repetidos
        if contratos_repetidos:
            df_repetidos = pd.concat(contratos_repetidos, ignore_index=True)
        else:
            df_repetidos = pd.DataFrame()
        
        # Criar DataFrame de resumo geral
        df_resumo = pd.DataFrame(resumo_geral)
        
        # Calcular totais para resumo
        total_geral = len(df_all_contratos)
        
        # Calcular auditados e não auditados
        if 'AUDITADO_TIPO' in df_all_contratos.columns:
            total_auditados = len(df_all_contratos[df_all_contratos['AUDITADO_TIPO'] == 'AUD'])
            total_nauditados = len(df_all_contratos[df_all_contratos['AUDITADO_TIPO'] == 'NAUD'])
        elif 'AUDITADO' in df_all_contratos.columns:
            df_all_contratos['AUDITADO'] = df_all_contratos['AUDITADO'].astype(str).str.upper().str.strip()
            total_auditados = len(df_all_contratos[df_all_contratos['AUDITADO'] == 'AUDI'])
            total_nauditados = len(df_all_contratos[df_all_contratos['AUDITADO'] == 'NAUD'])
        else:
            total_auditados = 0
            total_nauditados = 0
        
        total_repetidos = len(df_repetidos) if not df_repetidos.empty else 0
        
        # Adicionar linha de totais no resumo
        linha_totais = pd.DataFrame([{
            'ARQUIVO': 'TOTAL GERAL',
            'TIPO': '-',
            'TOTAL_LINHAS': total_geral,
            'CONTRATOS_UNICOS': total_geral - total_repetidos,
            'CONTRATOS_DUPLICADOS': total_repetidos,
            'BANCO': bank_name
        }])
        df_resumo = pd.concat([df_resumo, linha_totais], ignore_index=True)
        
        # Criar DataFrame de contratos por banco
        if contratos_por_banco:
            df_por_banco = pd.concat(contratos_por_banco, ignore_index=True)
            # Agrupar por banco e tipo, contando contratos únicos
            if 'CONTRATO' in df_por_banco.columns:
                df_por_banco = df_por_banco.groupby(['BANCO', 'TIPO_ARQUIVO']).agg({
                    'CONTRATO': 'nunique'
                }).reset_index()
                df_por_banco.columns = ['BANCO', 'TIPO_ARQUIVO', 'TOTAL_CONTRATOS']
            else:
                df_por_banco = df_por_banco.groupby(['BANCO', 'TIPO_ARQUIVO']).size().reset_index(name='TOTAL_CONTRATOS')
        else:
            df_por_banco = pd.DataFrame(columns=['BANCO', 'TIPO_ARQUIVO', 'TOTAL_CONTRATOS'])
        
        # Criar arquivo Excel consolidado
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Aba 1: Resumo Geral
            df_resumo.to_excel(writer, sheet_name='Resumo Geral', index=False)
            
            # Aba 2: Contratos Totais
            df_all_contratos.to_excel(writer, sheet_name='Contratos Totais', index=False)
            
            # Aba 3: Contratos Repetidos
            if not df_repetidos.empty:
                df_repetidos.to_excel(writer, sheet_name='Contratos Repetidos', index=False)
            else:
                pd.DataFrame({'Mensagem': ['Nenhum contrato repetido encontrado']}).to_excel(
                    writer, sheet_name='Contratos Repetidos', index=False
                )
            
            # Aba 4: Contratos por Banco
            if not df_por_banco.empty:
                df_por_banco.to_excel(writer, sheet_name='Contratos por Banco', index=False)
            else:
                pd.DataFrame({'Mensagem': ['Nenhum dado disponível']}).to_excel(
                    writer, sheet_name='Contratos por Banco', index=False
                )
        
        # Resetar ponteiro e ler dados
        output.seek(0)
        excel_data = output.read()
        output.close()
        
        # Retornar como StreamingResponse
        return StreamingResponse(
            io.BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=contratos_consolidados_{bank_type}.xlsx"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar contratos: {str(e)}"
        )

