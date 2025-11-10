import pandas as pd
import io
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

async def process_excel(file, tipo: str):
    """
    Processa arquivo Excel:
    - Filtra por coluna AUDITADO (valores "AUDI" ou "NAUD")
    - Marca contratos duplicados na coluna CONTRATO
    - Cria resumo com totais
    - Retorna arquivo Excel compatível
    """
    try:
        # Ler o arquivo Excel
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents), engine='openpyxl')
        
        # Verificar se as colunas necessárias existem
        if 'AUDITADO' not in df.columns:
            raise HTTPException(
                status_code=400,
                detail="Coluna 'AUDITADO' não encontrada no arquivo"
            )
        
        if 'CONTRATO' not in df.columns:
            raise HTTPException(
                status_code=400,
                detail="Coluna 'CONTRATO' não encontrada no arquivo"
            )
        
        # Filtrar por tipo (auditado ou nauditado)
        # Converter para string e tratar valores nulos
        df['AUDITADO'] = df['AUDITADO'].astype(str).str.upper().str.strip()
        
        if tipo == "auditado":
            df_filtrado = df[df['AUDITADO'] == 'AUDI'].copy()
        else:  # nauditado
            df_filtrado = df[df['AUDITADO'] == 'NAUD'].copy()
        
        # Marcar contratos duplicados na coluna CONTRATO
        df_filtrado['DUPLICADO'] = df_filtrado['CONTRATO'].duplicated(keep=False)
        
        # Criar resumo
        total_linhas = len(df_filtrado)
        total_duplicados = df_filtrado['DUPLICADO'].sum()
        total_unicos = total_linhas - total_duplicados
        
        resumo = pd.DataFrame({
            'Métrica': ['Total de Linhas', 'Contratos Únicos', 'Contratos Duplicados'],
            'Valor': [total_linhas, total_unicos, total_duplicados]
        })
        
        # Criar arquivo Excel em memória
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Aba com dados processados
            df_filtrado.to_excel(writer, sheet_name='Dados Processados', index=False)
            
            # Aba com resumo
            resumo.to_excel(writer, sheet_name='Resumo', index=False)
        
        # Resetar o ponteiro do buffer para o início
        output.seek(0)
        
        # Ler o conteúdo completo do buffer
        excel_data = output.getvalue()
        output.close()
        
        # Retornar como StreamingResponse
        return StreamingResponse(
            io.BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=planilha_processada_{tipo}.xlsx"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar arquivo: {str(e)}"
        )
