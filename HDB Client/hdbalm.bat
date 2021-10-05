@echo off

setlocal ENABLEEXTENSIONS
set EXENAME=%~f0

for %%F in ("%EXENAME%") do set dirname=%%~dpF

if DEFINED PYTHON_HOME goto :pythonHome
:noPythonFound
echo Error occurred! The environment variable PYTHON_HOME is not set!
echo -------------------------------------------------------------------------------
echo The hdbalm program requires a python version 3.6 or later. See SAP note 2990848.
echo You have the following options:
echo   1) Use the python version that comes with this hdbclient package and is 
echo      located at 
echo      "%dirname%Python". 
echo      Note that this python release does not include SSL.
echo   2) Download and install the latest Python release from  
echo      https://www.python.org/
echo Set the PYTHON_HOME environment variable to the installation directory that
echo contains the Python executable. 
echo -------------------------------------------------------------------------------
exit /B 1

:pythonHome
set PYTHON_EXE=%PYTHON_HOME%\python.exe
for /F "tokens=* USEBACKQ" %%F in (`%PYTHON_EXE% -c "import platform; major, minor, patch = platform.python_version_tuple(); print(major)"`) DO (
set MAJORVER=%%F
)
if "%MAJORVER%"=="2" (
	echo Python 2 detected.
    echo We strongly recommend you use Python 3 to prevent potential issues.
	echo See SAP note 2990848.
    echo Note that hdbalm is going to use installed version.
    %PYTHON_EXE% "%dirname%hdbalm.py" %*
)
if "%MAJORVER%"=="3" (
    %PYTHON_EXE% "%dirname%hdbalm3.py" %*
)
if "%MAJORVER%" NEQ "2" if "%MAJORVER%" NEQ "3" (
    goto :noPythonFound
)
exit /B %ERRORLEVEL%

endlocal