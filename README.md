# wallmart_EPL

PROCESO: ETL_SEASON.bat
AUTOR: MAURICIO GALLARDO
VERSION: V.0.1
FECHA: 21/01/2022

DESCRIPCION: PROCESO ETL PARA GENERACION DE REPORTES DE EPL
 
PARAMETROS DE ENTRADA: FEC_TEMPORADA: Fecha de la temporada (DDDD) D: Digito del 0-9

SALIDA (CARPETA src/reportes): 
  E0_TABLAPOSICION_DDDD.csv: REPORTE DE TABLA DE POSICIONES DIVISION E0
  E0_INDGOL_DDDD.csv       : REPORTE DE EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL DIVISION E0
  E0_CANGOLCONTRA_DDDD.csv : REPORTE DE EQUIPO MAS GOLEADO DIVISION E0 DIVISION E0  
  PL_TABLAPOSICION_DDDD.csv: REPORTE DE TABLA DE POSICIONES DIVISION PREMIER LEAGUE
  PL_INDGOL_DDDD.csv       : REPORTE DE EQUIPO MEJOR RELACION DE DISPAROS DE ARCO TERMINANDO EN GOL DIVISION PREMIER LEAGUE
  PL_CANGOLCONTRA_DDDD.csv : REPORTE DE EQUIPO MAS GOLEADO DIVISION E0 DIVISION PREMIER LEAGUE     


REQUERIMIENTOS:
	- Python version 3.7
	- Instalar libreria Pandas (pip install pandas)
	
INSTALACION:
	1- Copiar archivos en el directorio de la instalaciòn de Python (Por ejemplo C:\Python37)
	2- Debe quedar los archivos de la siguiente manera, por ejemplo:
		
		/Python37
		ETL_SEASON.bat
			/src
				ETL0_SEASON.py
				ETL1_SEASON.py
					/data
						season-0910_json
						season-1011_json
						season-1112_json
						(etc.)
					/pkl
					/report
		
	3- Ejecutar por comando de la siguiente manera
		C:\Python37>ETL_SEASON.bat 0910
		(NOTA: Donde 0910 es el numero de temporada que coincide con el dataset de entrada ubicado en /src/data)
	4- Esperar la ejecucion, presionar cualquier tecla y diriguirse a la carpeta src/report
