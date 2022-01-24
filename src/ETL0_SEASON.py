#####################################################################################################################
## PROCESO: ETL0_SEASON.py
## AUTOR: MAURICIO GALLARDO
## VERSION: V.0.1
## FECHA: 21/01/2022
##
## ##################################################################################################################
## DESCRIPCION: PROCESO DE EXTRACCION Y TRANSFORMACION DE LOS REPORTES PARA EPL
## ##################################################################################################################
## PARAMETROS DE ENTRADA: FEC_TEMPORADA: Fecha de la temporada (DDDD) D: Digito del 0-9
##
## SALIDA: 
##   season_DDDD.pkl: DATASET DE DATOS DE LA TEMPORADA PROCESADA   
##
#####################################################################################################################

import pandas as pd
import ast
import sys
import json
import os.path

if len(sys.argv)==1:
    print("La temporada que intenta cargar es vacia")
    quit()

if not os.path.isfile("src/data/season-"+sys.argv[1]+"_json.json"):
    print("La temporada que intenta cargar no existe")
    quit()


f = open("src/data/season-"+sys.argv[1]+"_json.json")

data = json.load(f)

filaArray = []

for row in data:        # ITERANDO EL ARCHIVO
    lineaActual = "["
    lineaActual = lineaActual + "\"" + row['Div'] + "\"" + ","
    lineaActual = lineaActual + "\"" + row['AwayTeam'] + "\"" + ","   
    lineaActual = lineaActual + "\"" + row['HomeTeam'] + "\"" + ","
    lineaActual = lineaActual + "\"" + str(row['FTR']) + "\"" + ","
    lineaActual = lineaActual + str(row['HST']) + ","
    lineaActual = lineaActual + str(row['AST']) + ","
    lineaActual = lineaActual + str(row['FTHG']) + ","
    lineaActual = lineaActual + str(row['FTAG']) + "]"
    
    filaArray.append(ast.literal_eval(lineaActual))

df = pd.DataFrame(filaArray, columns = ['Div','AwayTeam','HomeTeam','FTR','HST','AST','FTHG','FTAG'])     
df.to_pickle("src/pkl/season_"+sys.argv[1]+".pkl")

# CERRAR ARCHIVO
f.close()
