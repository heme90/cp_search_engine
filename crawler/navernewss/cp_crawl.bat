
echo off
start mongod  --dbpath c:\data\compathdata --logpath c:\data\compathdata\compath.log --port 27777
call C:\MyPython\news_engine\crawler\navernewss\newsone.py
call C:\MyPython\news_engine\crawler\navernewss\analyzetesttest.py
pause