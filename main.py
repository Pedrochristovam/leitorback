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
    files: List[UploadFile] = File(...)
):
    """
    Processa múltiplas planilhas Excel de contratos.
    
    Recebe:
    - bank_type: "bemge" ou "minas_caixa" (Form)
    - filter_type: "auditado", "nauditado" ou "todos" (Form)
    - files: Lista de arquivos Excel (File)
    
    Retorna:
    - Arquivo Excel consolidado (.xlsx) como StreamingResponse
    """
    try:
        return await process_contratos(files, bank_type, filter_type)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar contratos: {str(e)}"
        )
