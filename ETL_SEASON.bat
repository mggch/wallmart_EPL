echo off

python src\ETL0_SEASON.py %1
python src\ETL1_SEASON.py %1

pause