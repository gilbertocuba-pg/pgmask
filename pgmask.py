#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#Nombre: pgmask.py (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB, PSQL_TB, PSQL_CL)
#Descripcion: resive 7 parametros para general todos los campos de la tabla,<nombre o ip host>,<puerto>, <usuario>,<password>,<nombre baseDato>
#,<nombre Tabla>,<cantidad filas a procesar>
#Objetivo: Esta script fue creada para enmascarar o anonizar los datos de una tabla, para ello emplea 4 archivos configurables
#   names_female.cfg: describe nombres femeninos, names_male.cfg:describe nombres masculinos, last_names.cfg: describe apellidos, 
#   cities.cfg: describe direcciones; al final evita "Entregar" datos reales para la implementación de nuevas funcionalidades, marketing etc.
#Creada por: Gilberto Castillo,<gilberto.castillo@etecsa.cu> La habana, 2019
############################################################################################################

import sys
import psycopg2
import random
import time
import string
from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from random import randrange, seed



def nombreFeminas(file1, file3):		#Genera aleatoriamente una nombre femenino
	with open(file1,'r') as fnf, open(file3,'r') as fnl:  #Abrimos los archivos en modo Read (r)

    		linea1 = fnf.read().splitlines() # Almacenamos en variable linea1 el método read()
    		myline = random.choice(linea1)   # Realizamos la seleccion aletoria de la fila
    		myline =  myline.rstrip('\n')    # Eliminar el caracter de final de linea '\n'
    		linea2 = fnl.read().splitlines() # Almacenamos en variable linea2 el método read()
    		myline2 = random.choice(linea2)  # Realizamos la seleccion aletoria de la fila 
    		myline2 = myline2.rstrip('\n')    # Realizamos la seleccion aletoria de la fila
    		ambas = myline + ' ' + myline2 
    
    		return (ambas)

def nombreVarones(file2, file3):        	#Genera aleatoriamente una nombre masculino                   
	with open(file2,'r') as fnm, open(file3,'r') as fnl:  #Abrimos los archivos en modo Read (r)

    		linea1 = fnm.read().splitlines() # Almacenamos en variable linea1 el método read()
    		myline = random.choice(linea1)   # Realizamos la seleccion aletoria de la fila
    		myline =  myline.rstrip('\n')    # Eliminar el caracter de final de linea '\n'
    		linea2 = fnl.read().splitlines() # Almacenamos en variable linea2 el método read()
    		myline2 = random.choice(linea2)  # Realizamos la seleccion aletoria de la fila 
    		myline2 = myline2.rstrip('\n')    # Realizamos la seleccion aletoria de la fila
    		ambas = myline + ' ' + myline2 

    		return (ambas)


def strTimeProp(start, end, format, prop):

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))

#Operaciones con la fecha actual

fecham = date.today()		#Obtengo la fecha del sistema

yearsi = relativedelta(years=70)	##Para trabajar la persona debe tener < 70 años
yearsf = relativedelta(years=18)        ##Se inicia como trabajador > 18 años
fechafin = fecham - yearsi		##Restando limites a la fecha
fechain = fecham - yearsf		##Restando limites a la fecha
start = fechafin.strftime('%y/%m/%d')   ##Convertir la fecha en string [99/99/99]
end = fechain.strftime('%y/%m/%d')      ##Convertir la fecha en string [99/99/99]

def randomDate(start, end, prop):		#Genera aleatoriamente una fecha
    return strTimeProp(start, end, '%y/%m/%d', prop) ##Return una fecha aleatoria

fecha = randomDate(start, end, random.random()) ##Esto lo dejo aquí para no hacer tan oscuro el códdigo realmente debe esta dentro funcio CI

#fecha.replace('/', '') con esto logro eliminar los separadores de las fechas
#random.randrange(10) me devuelve un No. aleatorio entre 0-9

def CI(sexo):		##Crea un número de identidad según normas en Cuba
  if sexo == "f":
    idcu = fecha.replace('/', '') + str(random.randrange(10))+ str(random.randrange(10))+ str(random.randrange(10))+ str(random.randrange(10))+ "2"
  else:
    idcu = fecha.replace('/', '') + str(random.randrange(10))+ str(random.randrange(10))+ str(random.randrange(10))+ str(random.randrange(10))+ "5"    
  return idcu

def ccodigo(cant):  ##Crea un código aleatorio con la cantidad de digitos "cant" 
    n = 1
    codigo = ''
    while n<cant:
      codigo += str(random.randrange(cant))  #Añadir digito de manera sucesiva al final de la cadena
      n = n + 1
    return codigo

def Direccion(file4): 		#Genera aleatoriamente una direciones tomado de un archico de configuraciones cities.cfg
    with open(file4,'r') as fnc:  #Abrimos los archivos en modo Read (r)

    		linea1 = fnc.read().splitlines() # Almacenamos en variable linea1 el método read()
    		myline = random.choice(linea1)   #Selección aletoria de una dirección
    return myline               

def Edad():		#Genera aleatoriamente una Edad

    return str(randrange(0,75))

def Marca(): 		#Genera aleatoriamente una nombre de Marca

    VMarca=['HP','Patrio', 'Nokia.', 'Lenovo', 'Samsung', 'Sony', 'Daewoo']

    return VMarca[randrange(0,len(VMarca))]

def Saldo(): 		#Genera aleatoriamente una saldo de cuanta

    VSaldo=['1230.56', '32156.89', '48125.23', '51478.12', '189745.26', '5468921.78', '985467.36']

    return VSaldo[randrange(0,len(VSaldo))]

def Empresa(): 		#Genera aleatoriamente una saldo de cuanta

    VEmpresa=['Cisco', 'ETECSA', 'Gecyt', 'Desoft', 'Cimatel', 'Daisa', 'Palco']

    return VEmpresa[randrange(0,len(VEmpresa))]

def leer_datos(PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB, PSQL_TB, PSQL_CL):
    try:
       # Conectarse a la base de datos
       DSN = "host=%s port=%s user=%s password=%s dbname=%s" % (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB)
       con = psycopg2.connect(DSN)	# se crea la conexión	
       
       print("Inicio del Enmascarado de datos de la tabla: {}".format(PSQL_TB))
       #Cargar constantes de trabajo
       tb_dato = "{}_mask".format(PSQL_TB)
       if int(PSQL_CL) > 10000:		#Limitar numeros de registro a procesar
         PSQL_CL = 10000

       #Cursores para realizar operaciones sobre la base de datos
       cur = con.cursor()		
       curc = con.cursor()
       curcol = con.cursor()
       cursorm = con.cursor()
       curtruc = con.cursor()

       #Realizar operaciones sobre la base de datos
       creaesq = "CREATE SCHEMA IF NOT EXISTS pgmask AUTHORIZATION postgres;"  # consulta para crear esquema

       if PSQL_CL == 0:	 
         creatb = """CREATE TABLE IF NOT EXISTS pgmask.{}_mask AS SELECT * FROM {};""".format(PSQL_TB, PSQL_TB) # consulta para crear la tabla
       else:
         creatb = """CREATE TABLE IF NOT EXISTS pgmask.{}_mask AS SELECT * FROM {} LIMIT {};""".format(PSQL_TB, PSQL_TB, PSQL_CL) # consulta para crear la tabla
       cur.execute(creaesq)
       con.commit()
       cur.execute(creatb)
       con.commit()

       # Obtener la cantida de columnas de la tabla
       querycontar= "SELECT DISTINCT count(*) FROM pg_attribute a JOIN pg_class pgc ON pgc.oid = a.attrelid LEFT JOIN pg_index i ON (pgc.oid = i.indrelid AND i.indkey[0] = a.attnum) LEFT JOIN pg_description com on (pgc.oid = com.objoid AND a.attnum = com.objsubid) LEFT JOIN pg_attrdef def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum) WHERE a.attnum > 0 AND pgc.oid = a.attrelid AND pg_table_is_visible(pgc.oid) AND NOT a.attisdropped AND pgc.relname ='{}';".format(PSQL_TB)  ##nombre de la tabla
       curc.execute(querycontar)
       row = curc.fetchone()[0]
       
       # Obtener los nombres de columnas de la tabla 
       
       colquery= "SELECT DISTINCT a.attname as nombre_columna, format_type(a.atttypid, a.atttypmod) as tipo FROM pg_attribute a JOIN pg_class pgc ON pgc.oid = a.attrelid LEFT JOIN pg_index i ON (pgc.oid = i.indrelid AND i.indkey[0] = a.attnum) LEFT JOIN pg_description com on (pgc.oid = com.objoid AND a.attnum = com.objsubid) LEFT JOIN pg_attrdef def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum) WHERE a.attnum > 0 AND pgc.oid = a.attrelid AND pg_table_is_visible(pgc.oid) AND NOT a.attisdropped AND pgc.relname ='{}';".format(PSQL_TB)  ## Nombre de la tabla
       curcol.execute(colquery) 
       colnom = curcol.fetchall()
    
       # TRUNCATE para limpiar la tabla a enmascarar
       querytruc= "TRUNCATE pgmask.{}".format(tb_dato)
       curtruc.execute(querytruc)
       con.commit()

        # Hacer algo con los datos
       i=0
       a=0
       for i in range(0,int(PSQL_CL)):         ##For 1
         j = 0
         for j in range(0,row):                ##For 2
             # Validar los distintos tipos de datos
             if colnom[j][1] == 'text':
          	   dato = "\'"+ Direccion('cities.cfg') +"\'"      
             
             elif colnom[j][1].find('character varying') != -1:
                  if a==0:
                     dato = "\'"+nombreFeminas('names_female.cfg', 'last_names.cfg')+"\'"
                     a=1
                  else:
                     dato = "\'"+nombreVarones('names_male.cfg', 'last_names.cfg')+"\'"
                     a=0
             elif colnom[j][1] == 'integer':
                   dato = ccodigo(7);
             elif colnom[j][1] == 'smallint':
                   dato = ccodigo(2);
             
             elif colnom[j][1].find('numeric') != -1:
                   dato = Saldo();
                   
          	   
             if j==0:
                    querymask = "INSERT INTO pgmask.{}({}) VALUES ({})".format(tb_dato,colnom[j][0],dato)
                    muestra = dato
                    muestrad = colnom[j][0]
                    cursorm.execute(querymask)
                    con.commit()
             else:
                    querymask = "UPDATE pgmask.{} SET ({}) = ({}) WHERE ({}) = ({})".format(tb_dato,colnom[j][0],dato,muestrad,muestra)
                    cursorm.execute(querymask)
                    con.commit() 
             #Lazo del for 2
             j = j + 1;
                
         #Lazo del for 1
         i = i+1;
       print("Fin del Enmascarado de datos de la tabla: {}".format(PSQL_TB))
    except psycopg2.Error as e:
      print("Ocurrió un error al conectar a PostgreSQL: ", e)


###Llamado a las funciones####

if __name__== "__main__":
   
   leer_datos(*sys.argv[1:]);
   



####Llamado: python3 pgmask.py 'localhost' '5432' 'admin' 'admin' 'prueba' 'serv_basicos' 6

