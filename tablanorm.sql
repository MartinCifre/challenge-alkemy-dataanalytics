CREATE TABLE IF NOT EXISTS tablanorm(
    fecha_carga date PRIMARY KEY,
    cod_localidad Integer NOT NULL,
    id_provincia Integer NOT NULL,
    id_departamento Integer NOT NULL,
    categoría VARCHAR (255) NOT NULL,
    provincia VARCHAR (255) NOT NULL,
    localidad VARCHAR (255) NOT NULL,
    nombre VARCHAR (255) NOT NULL,
    domicilio VARCHAR (255) NOT NULL,
    código postal VARCHAR (255) NOT NULL,
    número de teléfono VARCHAR (255) NOT NULL,
    web VARCHAR (255) NOT NULL
)