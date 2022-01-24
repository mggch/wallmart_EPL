#####################################################################################################################
## PROCESO: ETL2_SEASON.py
## AUTOR: MAURICIO GALLARDO
## VERSION: V.0.1
## FECHA: 21/01/2022
##
## ##################################################################################################################
## DESCRIPCION: PROCESO DE CARGA DE LOS REPORTES PARA EPL (DISPARO ARCO Y MAS GOLEADOS POR TEMPORADA)
## ##################################################################################################################
## PARAMETROS DE ENTRADA: NO EXISTEN
##
## SALIDA (CARPETA src/report): 
##   E0_TABLAPOSICION_DDDD.csv: REPORTE DE TABLA DE POSICIONES DIVISION E0 POR TEMPORADA
##   PL_TABLAPOSICION_DDDD.csv: REPORTE DE TABLA DE POSICIONES DIVISION PREMIER LEAGUE POR TEMPORADA
##   E0_INDGOL.csv       : REPORTE DE EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL DIVISION E0 POR TEMPORADA
##   E0_CANGOLCONTRA.csv : REPORTE DE EQUIPO MAS GOLEADO DIVISION E0 DIVISION E0 POR TEMPORADA
##   PL_INDGOL.csv       : REPORTE DE EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL DIVISION PREMIER LEAGUE POR TEMPORADA
##   PL_CANGOLCONTRA.csv : REPORTE DE EQUIPO MAS GOLEADO DIVISION E0 DIVISION PREMIER LEAGUE POR TEMPORADA
##
#####################################################################################################################

import pandas as pd
import sys
import os.path
import os

pd.options.mode.chained_assignment = None

#############################################################################################
## TABLA DE POSICIONES POR TEMPORADA: E0
#############################################################################################

directory = os.fsencode("src/pkl")
   
df_REPORTE = pd.DataFrame(columns=['EQUIPO', 'SCORE', 'TEMPORADA'])

for file in os.listdir(directory):
     filename = os.fsdecode(file)
     if filename.startswith("E0_TABLAPOSICION_"):   
        df_REPORTE = df_REPORTE.append(pd.read_pickle("src/pkl/"+filename))

df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA'], ascending=False)
df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA','SCORE'], ascending=False)

df_REPORTE.to_csv("src/report/E0_TABLAPOSICION.csv", index=False)

#############################################################################################
## TABLA DE POSICIONES POR TEMPORADA: premier league
#############################################################################################

directory = os.fsencode("src/pkl")
   
df_REPORTE = pd.DataFrame(columns=['EQUIPO', 'SCORE', 'TEMPORADA'])

for file in os.listdir(directory):
     filename = os.fsdecode(file)
     if filename.startswith("PL_TABLAPOSICION_"):   
        df_REPORTE = df_REPORTE.append(pd.read_pickle("src/pkl/"+filename))

df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA'], ascending=False)
df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA','SCORE'], ascending=False)

df_REPORTE.to_csv("src/report/PL_TABLAPOSICION.csv", index=False)

#############################################################################################
## EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL: E0
#############################################################################################

directory = os.fsencode("src/pkl")
   
df_REPORTE = pd.DataFrame(columns=['EQUIPO', 'INDGOL', 'TEMPORADA'])

for file in os.listdir(directory):
     filename = os.fsdecode(file)
     if filename.startswith("E0_INDGOL_"):   
        df_REPORTE = df_REPORTE.append(pd.read_pickle("src/pkl/"+filename))

df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA'], ascending=False)
df_REPORTE.to_csv("src/report/E0_INDGOL.csv", index=False)
         
#############################################################################################
## EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL: premier league
#############################################################################################

directory = os.fsencode("src/pkl")
   
df_REPORTE = pd.DataFrame(columns=['EQUIPO', 'INDGOL', 'TEMPORADA'])

for file in os.listdir(directory):
     filename = os.fsdecode(file)
     if filename.startswith("PL_INDGOL_"): 
        df_REPORTE = df_REPORTE.append(pd.read_pickle("src/pkl/"+filename))

df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA'], ascending=False)
df_REPORTE.to_csv("src/report/PL_INDGOL.csv", index=False)

#############################################################################################
## EQUIPO MAS GOLEADO: E0
#############################################################################################

directory = os.fsencode("src/pkl")
   
df_REPORTE = pd.DataFrame(columns=['EQUIPO', 'CANGOLCONTRA', 'TEMPORADA'])

for file in os.listdir(directory):
     filename = os.fsdecode(file)
     if filename.startswith("E0_CANGOLCONTRA_"): 
        df_REPORTE = df_REPORTE.append(pd.read_pickle("src/pkl/"+filename))

df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA'], ascending=False)
df_REPORTE.to_csv("src/report/E0_CANGOLCONTRA.csv", index=False)

#############################################################################################
## EQUIPO MAS GOLEADO: premier league
#############################################################################################

directory = os.fsencode("src/pkl")
   
df_REPORTE = pd.DataFrame(columns=['EQUIPO', 'CANGOLCONTRA', 'TEMPORADA'])

for file in os.listdir(directory):
     filename = os.fsdecode(file)
     if filename.startswith("PL_CANGOLCONTRA_"): 
        df_REPORTE = df_REPORTE.append(pd.read_pickle("src/pkl/"+filename))

df_REPORTE = df_REPORTE.sort_values(by=['TEMPORADA'], ascending=False)
df_REPORTE.to_csv("src/report/PL_CANGOLCONTRA.csv", index=False)
