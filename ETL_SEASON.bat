echo off

python wallmart\ETL0_SEASON.py %1
python wallmart\ETL1_SEASON.py %1

pause