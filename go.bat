@echo off
set ROOT=C:\Users\bryany\Desktop\GitHub\flask-tutorial
call %ROOT%\venv\Scripts\activate.bat
cd %ROOT%
set FLASK_APP=flaskr && rem Windows
set FLASK_DEBUG=1 && rem reloads on any file changes
flask run --host=0.0.0.0 && rem allows connections from anywhere to :5000
