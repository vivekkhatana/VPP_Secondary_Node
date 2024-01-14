@echo off

rem Number of instances
set num_instances=2

rem Anaconda environment name
set conda_env=Sec_Node_Env

rem Python script to run
set python_script=secondary_node_Appr.py

rem Activate the Anaconda environment
call conda activate %conda_env%

rem Loop to open command windows and run the Python script
for /L %%i in (1,1,%num_instances%) do (
    start cmd /k python %python_script%
)

rem Deactivate the Anaconda environment (optional)
call conda deactivate