from fastapi import APIRouter, UploadFile, Form, HTTPException
from app.services.process_excel import process_excel

router = APIRouter()

@router.post("/upload/")
async def upload_file(file: UploadFile, tipo: str = Form(...)):
    """
    Rota para upload e processamento de arquivo Excel.
    
    Recebe:
    - file: arquivo Excel (UploadFile)
    - tipo: string com valor "auditado" ou "nauditado" (Form)
    
    Retorna:
    - Arquivo Excel processado (.xlsx) como blob
    """
    if tipo not in ["auditado", "nauditado"]:
        raise HTTPException(
            status_code=400,
            detail="O parâmetro 'tipo' deve ser 'auditado' ou 'nauditado'"
        )
    
    try:
        return await process_excel(file, tipo)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar arquivo: {str(e)}"
        )
