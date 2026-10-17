@echo off
cd /d "%~dp0"
if not defined FOURSIM_API_KEY (
  set /p FOURSIM_API_KEY=Enter your 4SIM API key: 
)
python -m pip install -r requirements.txt
start "" http://127.0.0.1:5050
python app.py
pause
