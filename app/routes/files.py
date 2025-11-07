from fastapi import APIRouter, UploadFile
from app.services.process_excel import process_excel

router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile):
    return await process_excel(file)
