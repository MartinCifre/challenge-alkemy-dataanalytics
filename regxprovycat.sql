CREATE TABLE IF NOT EXISTS regxprovycat(
    fecha_carga date PRIMARY KEY,  
    categoría VARCHAR (255) NOT NULL,
    provincia VARCHAR (255) NOT NULL,
    size Integer NOT NULL
   )