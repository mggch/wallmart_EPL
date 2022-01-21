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
import re
import ast
import os
import sys

s2 = ": "

testarray = []

with open("wallmart/data/season-"+sys.argv[1]+"_json.json") as fp:
        line = fp.readline()
        cnt = 1
        while line:
            line2 = line.strip()
                
            for match in re.finditer('{(.*?)}', line2, re.S):       # IDENTIFICA LA FILA
                linea1= match.group(1)
                xs = linea1.split(",")

                lineaActual = "["                                   # ESTRUCTURA LA FILA
                for s1 in xs:
                    if lineaActual == "[":
                        lineaActual=lineaActual + s1[s1.index(s2) + len(s2):]
                    else:
                        lineaActual=lineaActual + "," + s1[s1.index(s2) + len(s2):]
                lineaActual = lineaActual + "]"
                testarray.append(ast.literal_eval(lineaActual))

                
            line = fp.readline()
            cnt += 1

df = pd.DataFrame(testarray, columns = ['AC','AF','AR','AS','AST','AY','AwayTeam','B365A','B365D','B365H','BSA','BSD','BSH','BWA','BWD','BWH','Bb1X2','BbAH','BbAHh','BbAv<2.5','BbAv>2.5','BbAvA','BbAvAHA','BbAvAHH','BbAvD','BbAvH','BbMx<2.5','BbMx>2.5','BbMxA','BbMxAHA','BbMxAHH','BbMxD','BbMxH','BbOU','Date','Div','FTAG','FTHG','FTR','GBA','GBD','GBH','HC','HF','HR','HS','HST','HTAG','HTHG','HTR','HY','HomeTeam','IWA','IWD','IWH','LBA','LBD','LBH','Referee','SBA','SBD','SBH','SJA','SJD','SJH','VCA','VCD','VCH','WHA','WHD','WHH'])     
df.to_pickle("wallmart/pkl/season_"+sys.argv[1]+".pkl")
