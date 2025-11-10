from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io

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
