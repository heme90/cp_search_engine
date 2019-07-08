
echo off

start mongod  --dbpath c:\data\compathdata --logpath c:\data\compathdata\compath.log --port 27777
:1
call C:\MyPython\news_engine\crawler\navernewss\table_analyze.py
timeout -t 86400 /nobreak
goto 1
pause