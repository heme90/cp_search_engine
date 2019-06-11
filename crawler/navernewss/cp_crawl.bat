
echo off
start mongod  --dbpath c:\data\compathdata --logpath c:\data\compathdata\compath.log --port 27777
call C:\MyPython\mpython\com\crawler\navernewss\newsone.py
call C:\MyPython\mpython\com\crawler\navernewss\analyzetesttest.py
pause