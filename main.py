from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
from app.services.process_contratos import process_contratos

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "API do Leitor de Arquivos rodando!"}

@app.post("/processar/")
async def processar_excel(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        total_linhas = len(df)
        colunas = list(df.columns)
        return {
            "mensagem": "Arquivo processado com sucesso!",
            "total_linhas": total_linhas,
            "colunas": colunas
        }
    except Exception as e:
        return {"erro": str(e)}

@app.post("/processar_contratos/")
async def processar_contratos_endpoint(
    bank_type: str = Form(...),
    filter_type: str = Form(...),
    file_type: str = Form("todos"),
    period_filter: str = Form("todos"),
    files: List[UploadFile] = File(...)
):
    """
    Processa múltiplas planilhas Excel de contratos.
    
    Recebe:
    - bank_type: "bemge" ou "minas_caixa" (Form)
    - filter_type: "auditado", "nauditado" ou "todos" (Form)
    - file_type: "3026-11", "3026-12", "3026-15" ou "todos" (Form)
    - period_filter: "todos" ou "ultimos_2_meses" (Form) - NOVO
    - files: Lista de arquivos Excel (File)
    
    Retorna:
    - Arquivo Excel consolidado (.xlsx) como StreamingResponse
    """
    # Validar file_type
    if file_type not in ["3026-11", "3026-12", "3026-15", "todos"]:
        raise HTTPException(
            status_code=400,
            detail="file_type deve ser '3026-11', '3026-12', '3026-15' ou 'todos'"
        )
    
    # Validar period_filter
    if period_filter not in ["todos", "ultimos_2_meses"]:
        raise HTTPException(
            status_code=400,
            detail="period_filter deve ser 'todos' ou 'ultimos_2_meses'"
        )
    
    try:
        return await process_contratos(files, bank_type, filter_type, file_type, period_filter)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar contratos: {str(e)}"
        )
