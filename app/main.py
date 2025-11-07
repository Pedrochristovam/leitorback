from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import files

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(files.router)

@app.get("/")
def read_root():
    return {"message": "Backend rodando 🚀"}
