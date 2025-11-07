from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os

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
def processar_excel(caminho: str):
    if not os.path.exists(caminho):
        return {"erro": "Arquivo não encontrado."}

    try:
        df = pd.read_excel(caminho)
        total_linhas = len(df)
        return {"total_linhas": total_linhas}
    except Exception as e:
        return {"erro": str(e)}
