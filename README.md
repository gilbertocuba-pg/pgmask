# pgmask
Anonizador o emascarado de datos en Postgresql
pgmask.py (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB, PSQL_TB, PSQL_CL)
#Descripcion: resive 7 parametros para general todos los campos de la tabla,<nombre o ip host>,<puerto>, <usuario>,<password>,<nombre baseDato>,<nombre Tabla>,<cantidad filas a procesar>
Objetivo: Esta script fue creada para enmascarar o anonizar los datos de una tabla, para ello emplea 4 archivos configurables
   names_female.cfg: describe nombres femeninos, names_male.cfg:describe nombres masculinos, last_names.cfg: describe apellidos, 
   cities.cfg: describe direcciones; al final evita "Entregar" datos reales para la implementación de nuevas funcionalidades, marketing etc.
Creada por: Gilberto Castillo,<gilberto.castillo@etecsa.cu> La habana, 2019
  
  Ejemplo de su uso: 
  python3 pgmask.py 'localhost' '5432' 'postgres' 'postgres' 'prueba' 'serv_basicos' 6
  
