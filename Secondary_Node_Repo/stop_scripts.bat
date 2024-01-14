@echo off

rem Specify the name of the Python script without the extension
set python_script_name=secondary_node_Appr.py

rem Stop all instances of the specified Python script
taskkill /IM python.exe /FI "WINDOWTITLE eq %python_script_name%*"