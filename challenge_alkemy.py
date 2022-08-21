from datetime import date
from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd
import requests

#URL de los datasets
museos_url = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv"
cines_url="https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv"
bibliotecas_url="https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv"


"""Descarga y almacenamiento de archivos fuente para poder procesar"""

museos = {"name": "museos", "url": museos_url}
cines = {"name": "cines", "url": cines_url}
bibliotecas = {"name": "bibliotecas", "url": bibliotecas_url}

def descarga(cat_dicc):
    """descarga archivo fuente con la url, crea el directorio y lo guarda. Luego devuelve ruta de descarga.
    Args: nombre de la fuente

    returns: ruta del archivo descargado
    """
    rutacsv = "{categoria}/{año}-{mes:02d}/{categoria}-{dia:02d}-{mes:02d}-{año}.csv"
    r = requests.get(cat_dicc["url"])
    r.encoding = "utf-8"
    hoy = date.today()
    ruta = Path(rutacsv.format(categoria=cat_dicc["name"] , año=hoy.year , mes=hoy.month , dia = hoy.day))
    ruta.parent.mkdir(parents=True, exist_ok=True)

    with open (ruta,"w") as f_out:
        f_out.write(r.text) 
    return ruta

dfmuseos = pd.read_csv(descarga(museos))
dfcines = pd.read_csv(descarga(cines))
dfbibliotecas = pd.read_csv(descarga(bibliotecas))

"""Procesamiento de los datos"""

# Normalizacion de datos

dfmuseos.rename(columns={'Cod_Loc':'cod_localidad',
                        'IdProvincia':'id_provincia',
                        'IdDepartamento':'id_departamento',
                        'categoria':'categoría',
                        'direccion':'domicilio',
                        'CP':'código postal',
                        'telefono':'número de teléfono',
                        'Mail':'mail',
                        'Web':'web'},
               inplace=True)

dfcines.rename(columns={'Cod_Loc':'cod_localidad',
                        'IdProvincia':'id_provincia',
                        'IdDepartamento':'id_departamento',
                        'Categoría':'categoría',
                        'Provincia':'provincia',
                        'Localidad':'localidad',
                        'Nombre':'nombre',                      
                        'Dirección':'domicilio',
                        'CP':'código postal',
                        'Teléfono':'número de teléfono',
                        'Mail':'mail',
                        'Web':'web'},
               inplace=True)

dfbibliotecas.rename(columns={'Cod_Loc':'cod_localidad',
                        'IdProvincia':'id_provincia',
                        'IdDepartamento':'id_departamento',
                        'Categoría':'categoría',
                        'Provincia':'provincia',
                        'Localidad':'localidad',
                        'Nombre':'nombre',                      
                        'Domicilio':'domicilio',
                        'CP':'código postal',
                        'Teléfono':'número de teléfono',
                        'Mail':'mail',
                        'Web':'web'},
               inplace=True)

# Tabla unica con columnas normalizadas
columnasnorm = ["cod_localidad", "id_provincia", "id_departamento", "categoría","provincia","localidad","nombre","domicilio","código postal","número de teléfono","mail","web"]
tablanorm = pd.concat([dfmuseos[columnasnorm], dfcines[columnasnorm],  dfbibliotecas[columnasnorm]])  
tablanorm

#Cantidad de registros totales por fuente
dfnorm ={ "museos": dfmuseos, "cines": dfcines, "bibliotecas": dfbibliotecas}
lst = list()
for name, df in dfnorm.items():
  lst.append({"fuente":name , "cantidad": df.size})
regxfuente = pd.DataFrame(lst)


#Cantidad de registros totales por categoría
regxcat = tablanorm.groupby("categoría",as_index=False).size()


#Cantidad de registros por provincia y categoría
regxprovycat =tablanorm.groupby(["categoría","provincia"],as_index=False).size()


#Procesar la información de cines para poder crear una tabla
tablacines = dfcines.groupby("provincia",as_index=False).count()[["provincia","Pantallas","Butacas","espacio_INCAA"]]

"""Almacenamiento de los datos procesados en la base de datos"""

engine = create_engine("postgresql+psycopg2://")

tablanorm.to_sql("tablanorm", con=engine, index=False, if_exists="replace")
regxfuente.to_sql("regxfuente", con=engine, index=False, if_exists="replace")
regxcat.to_sql("regxcat", con=engine, index=False, if_exists="replace")
regxprovycat.to_sql("regxprovycat", con=engine, index=False, if_exists="replace")
tablacines.to_sql("tablacines", con=engine, index=False, if_exists="replace")
