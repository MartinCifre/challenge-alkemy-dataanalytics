from sqlalchemy import create_engine
from sqlalchemy.sql import text
import logging
from pathlib import Path

BASE_FILE_DIR = Path("/tmp")
ROOT_DIR = Path().resolve().parent
SQL_DIR = ROOT_DIR / "challenge/sql"

NORM_TABLE_NAME = "tablanorm"
CINES_TABLE_NAME = "tablacines"
FUENTE_TABLE_NAME = "regxfuente"
CATEGORIA_TABLE_NAME = "regxcat"
CATYPROV_TABLE_NAME = "regxcatyprov" 

TABLE_NAMES = [
    NORM_TABLE_NAME,
    CINES_TABLE_NAME,
    FUENTE_TABLE_NAME,
    CATEGORIA_TABLE_NAME,
    CATYPROV_TABLE_NAME,
]

DB_CONNSTR=postgresql+psycopg2://postgres:postgres@localhost/data_analytics

ROOT_DIR = Path().resolve().parent
config= autoconfig(search_path=ROOT_DIR)
DB_CONNSTR=config("DB_CONNSTR")

engine = create_engine(DB_CONNSTR)
log = logging.getLogger

def crear_tablas():
    """ Crear las tablas de la bd"""
    with engine.connect() as con:
        for file in TABLE_NAMES:
            log.info(f"Creando tabla {file}")
            with open(SQL_DIR / f"{file}.sql") as f:
                query = text.(f.read())

            con.execute(f"Eliminar tabla si existe {file}")    
            con.execute(query)    

if __name__ == "__main__":
    crear_tablas