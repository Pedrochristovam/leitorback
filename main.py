from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from typing import List, Optional
import pandas as pd
import io
from app.services.process_contratos import process_contratos
from app.routes import files

app = FastAPI()

# CONFIGURAÇÃO CORS - Melhorada com origens específicas
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://leitorarquivos.onrender.com",
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir rotas de arquivos
app.include_router(files.router)

# Tratamento de erros global
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"erro": exc.detail}
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"erro": "Dados inválidos", "detalhes": str(exc)}
    )

@app.get("/")
def home():
    return {"message": "API do Leitor de Arquivos rodando!"}

@app.get("/health")
async def health_check():
    """Endpoint de health check para verificar se o servidor está funcionando"""
    return {"status": "ok", "message": "Servidor funcionando"}

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
    period_filter_enabled: str = Form("false"),
    reference_date: Optional[str] = Form(None),
    months_back: int = Form(2),
    files: List[UploadFile] = File(...)
):
    """
    Processa múltiplas planilhas Excel de contratos.
    
    Recebe:
    - bank_type: "bemge" ou "minas_caixa" (Form)
    - filter_type: "auditado", "nauditado" ou "todos" (Form)
    - file_type: "3026-11", "3026-12", "3026-15" ou "todos" (Form)
    - period_filter_enabled: "true" ou "false" (Form) - Ativa filtro de período
    - reference_date: "YYYY-MM-DD" (Form) - Data de referência para o filtro
    - months_back: 1, 2, 3, 4, 5, 6 ou 12 (Form) - Meses para trás
    - files: Lista de arquivos Excel (File)
    
    Retorna:
    - Arquivo Excel consolidado (.xlsx) como StreamingResponse
    """
    # Validar file_type no servidor
    valid_file_types = ["3026-11", "3026-12", "3026-15", "todos"]
    file_type_normalized = file_type.strip() if file_type else ""
    if file_type_normalized not in valid_file_types:
        raise HTTPException(
            status_code=400,
            detail=f"file_type inválido: '{file_type}'. Valores aceitos: {', '.join(valid_file_types)}"
        )
    file_type = file_type_normalized
    
    # Validar period_filter_enabled
    if period_filter_enabled not in ["true", "false"]:
        raise HTTPException(
            status_code=400,
            detail="period_filter_enabled deve ser 'true' ou 'false'"
        )
    
    # Validar months_back
    if months_back not in [1, 2, 3, 4, 5, 6, 12]:
        raise HTTPException(
            status_code=400,
            detail="months_back deve ser 1, 2, 3, 4, 5, 6 ou 12"
        )
    
    # Se filtro de período está ativo, reference_date é obrigatório
    if period_filter_enabled == "true" and not reference_date:
        raise HTTPException(
            status_code=400,
            detail="reference_date é obrigatório quando period_filter_enabled é 'true'"
        )
    
    try:
        return await process_contratos(
            files, 
            bank_type, 
            filter_type, 
            file_type, 
            period_filter_enabled,
            reference_date,
            months_back
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar contratos: {str(e)}"
        )
