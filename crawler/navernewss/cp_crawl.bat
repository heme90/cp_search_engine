
echo off

start mongod  --dbpath c:\data\compathdata --logpath c:\data\compathdata\compath.log --port 27777
:1
call C:\MyPython\news_engine\crawler\navernewss\newsone.py
call C:\MyPython\news_engine\crawler\navernewss\analyzetesttest.py
call C:\MyPython\news_engine\crawler\navernewss\pymatrix_mk.py
timeout -t 600 /nobreak
goto 1
pause
