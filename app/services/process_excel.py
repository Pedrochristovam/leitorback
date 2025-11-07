import pandas as pd
from fastapi.responses import JSONResponse

async def process_excel(file):
    try:
        df = pd.read_excel(file.file)
        total_linhas = len(df)
        colunas = list(df.columns)
        return JSONResponse(content={
            "status": "sucesso",
            "total_linhas": total_linhas,
            "colunas": colunas
        })
    except Exception as e:
        return JSONResponse(content={"status": "erro", "mensagem": str(e)}, status_code=400)
