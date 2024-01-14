@echo off

rem Number of instances
set num_instances=8

rem Python script to run
set python_script=secondary_node_Appr.py

rem Loop to open command windows and run the Python script
for /L %%i in (1,1,%num_instances%) do (
    start cmd /k python %python_script%
)
