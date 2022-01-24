#####################################################################################################################
## PROCESO: ETL1_SEASON.py
## AUTOR: MAURICIO GALLARDO
## VERSION: V.0.1
## FECHA: 21/01/2022
##
## ##################################################################################################################
## DESCRIPCION: PROCESO DE CARGA DE LOS REPORTES PARA EPL
## ##################################################################################################################
## PARAMETROS DE ENTRADA: FEC_TEMPORADA: Fecha de la temporada (DDDD) D: Digito del 0-9
##
## SALIDA (CARPETA src/pkl): 
##   E0_TABLAPOSICION_DDDD.pkl: DATASET DE TABLA DE POSICIONES DIVISION E0
##   PL_TABLAPOSICION_DDDD.pkl: DATASET DE TABLA DE POSICIONES DIVISION PREMIER LEAGUE
##   E0_INDGOL_DDDD.pkl       : DATASET DE EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL DIVISION E0
##   E0_CANGOLCONTRA_DDDD.pkl : DATASET DE EQUIPO MAS GOLEADO DIVISION E0 DIVISION E0  
##   PL_INDGOL_DDDD.pkl       : DATASET DE EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL DIVISION PREMIER LEAGUE
##   PL_CANGOLCONTRA_DDDD.pkl : DATASET DE EQUIPO MAS GOLEADO DIVISION E0 DIVISION PREMIER LEAGUE    
##
#####################################################################################################################

import pandas as pd
import sys
import os.path

pd.options.mode.chained_assignment = None

#############################################################################################
## FUNCION GET_SCORE PARA CALCULO DE PUNTAJES DE LA TABLA DE POSICIONES
#############################################################################################

def get_score_home(FTR):
    if FTR == 'D':
        return 1        
    elif FTR == 'H':
        return 3;
    else:
        return 0

def get_score_away(FTR):
    if FTR == 'D':
        return 1        
    elif FTR == 'A':
        return 3;
    else:
        return 0


if not os.path.isfile("src/pkl/season_"+sys.argv[1]+".pkl"):
    quit()

#############################################################################################
## CARGA DE DATASET DE SCORE ACTUAL
#############################################################################################

unpickled_df = pd.read_pickle("src/pkl/season_"+sys.argv[1]+".pkl")

#############################################################################################
## OBTENCION DE CAMPOS IMPORTANTES PARA LOS CALCULOS DE LOS INDICADORES Y REPORTES
#############################################################################################

df = unpickled_df[['Div','AwayTeam', 'HomeTeam','FTR','HST','AST','FTHG','FTAG']]

#############################################################################################
## TABLA DE POSICION DE DIVISION: E0
#############################################################################################

df_E0 = df[df['Div'] == 'E0']

if not df_E0.empty:

    df_E0['SCORE'] = df_E0.apply(lambda x: get_score_home(x['FTR']), axis=1)

    df_HomeTeam = df_E0.groupby('HomeTeam')['SCORE'].sum().reset_index()
    df_HomeTeam.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True)
    df_HomeTeam.set_index('EQUIPO')

    df_E0['SCORE'] = df_E0.apply(lambda x: get_score_away(x['FTR']), axis=1)

    df_AwayTeam = df_E0.groupby('AwayTeam')['SCORE'].sum().reset_index()
    df_AwayTeam.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)
    df_AwayTeam.set_index('EQUIPO')

    df_SCORE = df_HomeTeam.append(df_AwayTeam)
    df_SCORE = df_SCORE.groupby('EQUIPO')['SCORE'].sum().reset_index()

    df_SCORE = df_SCORE.sort_values(by=['SCORE'], ascending=False)
    
    df_SCORE['TEMPORADA'] = sys.argv[1]

    df_SCORE.to_pickle("src/pkl/E0_TABLAPOSICION_"+sys.argv[1]+".pkl")
    
#############################################################################################
## TABLA DE POSICION DE DIVISION: premier league
#############################################################################################

df_PL = df[df['Div'] == 'premier league']

if not df_PL.empty:
    
    df_PL['SCORE'] = df_PL.apply(lambda x: get_score_home(x['FTR']), axis=1)

    df_HomeTeam = df_PL.groupby('HomeTeam')['SCORE'].sum().reset_index()
    df_HomeTeam.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True)
    df_HomeTeam.set_index('EQUIPO')

    df_PL['SCORE'] = df_PL.apply(lambda x: get_score_away(x['FTR']), axis=1)

    df_AwayTeam = df_PL.groupby('AwayTeam')['SCORE'].sum().reset_index()
    df_AwayTeam.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)
    df_AwayTeam.set_index('EQUIPO')

    df_SCORE = df_HomeTeam.append(df_AwayTeam)
    df_SCORE = df_SCORE.groupby('EQUIPO')['SCORE'].sum().reset_index()

    df_SCORE = df_SCORE.sort_values(by=['SCORE'], ascending=False)

    df_SCORE['TEMPORADA'] = sys.argv[1]

    df_SCORE.to_pickle("src/pkl/PL_TABLAPOSICION_"+sys.argv[1]+".pkl")
    
#############################################################################################
## EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL: E0
#############################################################################################

df_E0 = df[df['Div'] == 'E0']

if not df_E0.empty:

    df_HomeTeam_HST = df_E0.groupby('HomeTeam')['HST'].sum().reset_index()       # Disparos al arco 
    df_HomeTeam_HST.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True)
    df_HomeTeam_HST.set_index('EQUIPO')

    df_HomeTeam_FTHG = df_E0.groupby('HomeTeam')['FTHG'].sum().reset_index()     # Goles Marcados 
    df_HomeTeam_FTHG.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True)
    df_HomeTeam_FTHG.set_index('EQUIPO')

    df_HomeTeam = df_HomeTeam_HST.merge(df_HomeTeam_FTHG, how='left')
    
    df_AwayTeam_AST = df_E0.groupby('AwayTeam')['AST'].sum().reset_index()       # Disparos al arco 
    df_AwayTeam_AST.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)
    df_AwayTeam_AST.set_index('EQUIPO')
    
    df_AwayTeam_FTAG = df_E0.groupby('AwayTeam')['FTAG'].sum().reset_index()     # Goles Marcados 
    df_AwayTeam_FTAG.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)
    df_AwayTeam_FTAG.set_index('EQUIPO')

    df_AwayTeam = df_AwayTeam_AST.merge(df_AwayTeam_FTAG, how='left')   
    
    df_INDGOL = df_HomeTeam.merge(df_AwayTeam, how='left') 
    df_INDGOL['INDGOL'] = (df_INDGOL['FTHG']+df_INDGOL['FTAG'])/(df_INDGOL['HST']+df_INDGOL['AST'])
    df_INDGOL = df_INDGOL.sort_values(by=['INDGOL'], ascending=False).head(1)

    df_INDGOL['TEMPORADA'] = sys.argv[1]
    
    df_INDGOL.drop('HST', axis=1, inplace=True)
    df_INDGOL.drop('FTHG', axis=1, inplace=True)
    df_INDGOL.drop('AST', axis=1, inplace=True)
    df_INDGOL.drop('FTAG', axis=1, inplace=True)
    
    df_INDGOL.to_pickle("src/pkl/E0_INDGOL_"+sys.argv[1]+".pkl")
   
#############################################################################################
## EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL: premier league
#############################################################################################

df_PL = df[df['Div'] == 'premier league']

if not df_PL.empty:

    df_HomeTeam_HST = df_PL.groupby('HomeTeam')['HST'].sum().reset_index()       # Disparos al arco 
    df_HomeTeam_HST.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True)
    df_HomeTeam_HST.set_index('EQUIPO')

    df_HomeTeam_FTHG = df_PL.groupby('HomeTeam')['FTHG'].sum().reset_index()     # Goles Marcados 
    df_HomeTeam_FTHG.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True)
    df_HomeTeam_FTHG.set_index('EQUIPO')

    df_HomeTeam = df_HomeTeam_HST.merge(df_HomeTeam_FTHG, how='left')
    
    
    df_AwayTeam_AST = df_PL.groupby('AwayTeam')['AST'].sum().reset_index()       # Disparos al arco 
    df_AwayTeam_AST.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)
    df_AwayTeam_AST.set_index('EQUIPO')

    
    df_AwayTeam_FTAG = df_PL.groupby('AwayTeam')['FTAG'].sum().reset_index()     # Goles Marcados 
    df_AwayTeam_FTAG.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)
    df_AwayTeam_FTAG.set_index('EQUIPO')

    df_AwayTeam = df_AwayTeam_AST.merge(df_AwayTeam_FTAG, how='left')   
    
    df_INDGOL = df_HomeTeam.merge(df_AwayTeam, how='left') 
    df_INDGOL['INDGOL'] = (df_INDGOL['FTHG']+df_INDGOL['FTAG'])/(df_INDGOL['HST']+df_INDGOL['AST'])
    df_INDGOL = df_INDGOL.sort_values(by=['INDGOL'], ascending=False).head(1)

    df_INDGOL['TEMPORADA'] = sys.argv[1]
    
    df_INDGOL.drop('HST', axis=1, inplace=True)
    df_INDGOL.drop('FTHG', axis=1, inplace=True)
    df_INDGOL.drop('AST', axis=1, inplace=True)
    df_INDGOL.drop('FTAG', axis=1, inplace=True)

    df_INDGOL.to_pickle("src/pkl/PL_INDGOL_"+sys.argv[1]+".pkl")
    
#############################################################################################
## EQUIPO MAS GOLEADO: E0
#############################################################################################

df_E0 = df[df['Div'] == 'E0']

if not df_E0.empty:

    df_HomeTeam_FTAG = df_E0.groupby('HomeTeam')['FTAG'].sum().reset_index()     # Goles Marcados Contra  
    df_HomeTeam_FTAG.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True) 
    df_HomeTeam_FTAG.set_index('EQUIPO')
       
    df_AwayTeam_FTHG = df_E0.groupby('AwayTeam')['FTHG'].sum().reset_index()     # Goles Marcados Contra
    df_AwayTeam_FTHG.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)    
    df_AwayTeam_FTHG.set_index('EQUIPO')

    df_CANGOLCONTRA = df_HomeTeam_FTAG.merge(df_AwayTeam_FTHG, how='left')   
    
    df_CANGOLCONTRA['CANGOLCONTRA'] = df_CANGOLCONTRA['FTAG']+df_CANGOLCONTRA['FTHG']
    df_CANGOLCONTRA = df_CANGOLCONTRA.sort_values(by=['CANGOLCONTRA'], ascending=False).head(1)

    df_CANGOLCONTRA['TEMPORADA'] = sys.argv[1]
    
    df_CANGOLCONTRA.drop('FTAG', axis=1, inplace=True)
    df_CANGOLCONTRA.drop('FTHG', axis=1, inplace=True)
    
    df_CANGOLCONTRA.to_pickle("src/pkl/E0_CANGOLCONTRA_"+sys.argv[1]+".pkl")

#############################################################################################
## EQUIPO MAS GOLEADO: premier league
#############################################################################################

df_PL = df[df['Div'] == 'premier league']

if not df_PL.empty:

    df_HomeTeam_FTAG = df_PL.groupby('HomeTeam')['FTAG'].sum().reset_index()     # Goles Marcados Contra
    df_HomeTeam_FTAG.rename(columns={'HomeTeam': 'EQUIPO'},inplace=True) 
    df_HomeTeam_FTAG.set_index('EQUIPO')
 
    df_AwayTeam_FTHG = df_PL.groupby('AwayTeam')['FTHG'].sum().reset_index()     # Goles Marcados Contra
    df_AwayTeam_FTHG.rename(columns={'AwayTeam': 'EQUIPO'},inplace=True)    
    df_AwayTeam_FTHG.set_index('EQUIPO')

    df_CANGOLCONTRA = df_HomeTeam_FTAG.merge(df_AwayTeam_FTHG, how='left')   
    
    df_CANGOLCONTRA['CANGOLCONTRA'] = df_CANGOLCONTRA['FTAG']+df_CANGOLCONTRA['FTHG']
    df_CANGOLCONTRA = df_CANGOLCONTRA.sort_values(by=['CANGOLCONTRA'], ascending=False).head(1)
    
    df_CANGOLCONTRA['TEMPORADA'] = sys.argv[1]    

    df_CANGOLCONTRA.drop('FTAG', axis=1, inplace=True)
    df_CANGOLCONTRA.drop('FTHG', axis=1, inplace=True)

    df_CANGOLCONTRA.to_pickle("src/pkl/PL_CANGOLCONTRA_"+sys.argv[1]+".pkl")
    
