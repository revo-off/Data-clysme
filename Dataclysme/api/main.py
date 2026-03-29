import os
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker, Session

# Configuration de la BDD
DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:my-secret-pw@localhost:3306/dataclysme")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

app = FastAPI(
    title="Dataclysme API",
    description="API de lecture (GET uniquement) pour les datamarts Gold (Risques, Tourisme, Agriculture).",
    version="1.0.0"
)

# Dependance pour obtenir la session DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def fetch_table_data(db: Session, table_name: str, limit: int = 100, year: int = None, country: str = None):
    try:
        # On inspecte dynamiquement la table pour eviter de tout re-declarer, vu que l'on fait que du GET
        if not inspect(engine).has_table(table_name):
            raise HTTPException(status_code=404, detail=f"Table {table_name} introuvable.")
            
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.select()
        
        # Filtres optionnels
        if year is not None:
            if 'year' in table.c:
                query = query.where(table.c.year == year)
        if country is not None:
            if 'country' in table.c:
                query = query.where(table.c.country == country)
                
        # Limite pour eviter les surcharges
        query = query.limit(limit)
        
        result = db.execute(query).mappings().fetchall()
        return [dict(row) for row in result]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/", tags=["Infos"])
def read_root():
    return {
        "message": "Bienvenue sur l'API Dataclysme !",
        "endpoints": [
            "/risks",
            "/tourism",
            "/agriculture"
        ],
        "doc": "/docs"
    }

@app.get("/risks", tags=["Datamarts"])
def get_risks(
    limit: int = Query(100, description="Nombre max de lignes"), 
    year: int = Query(None, description="Filtrer par année"), 
    country: str = Query(None, description="Filtrer par pays"),
    db: Session = Depends(get_db)
):
    """
    Récupérer les données du Datamart des Risques (dm_risks)
    """
    return fetch_table_data(db, "dm_risks", limit, year, country)

@app.get("/tourism", tags=["Datamarts"])
def get_tourism(
    limit: int = Query(100, description="Nombre max de lignes"), 
    year: int = Query(None, description="Filtrer par année"), 
    country: str = Query(None, description="Filtrer par pays"),
    db: Session = Depends(get_db)
):
    """
    Récupérer les données du Datamart Tourisme (dm_tourism)
    """
    return fetch_table_data(db, "dm_tourism", limit, year, country)

@app.get("/agriculture", tags=["Datamarts"])
def get_agriculture(
    limit: int = Query(100, description="Nombre max de lignes"), 
    year: int = Query(None, description="Filtrer par année"), 
    country: str = Query(None, description="Filtrer par pays"),
    db: Session = Depends(get_db)
):
    """
    Récupérer les données du Datamart Agriculture (dm_agriculture)
    """
    return fetch_table_data(db, "dm_agriculture", limit, year, country)
