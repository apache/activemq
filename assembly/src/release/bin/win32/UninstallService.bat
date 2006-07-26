@echo off
setlocal

rem Java Service Wrapper general NT service uninstall script

if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
rem
rem Find the application home.
rem
rem %~dp0 is location of current script under NT
set _REALPATH=%~dp0

set ACTIVEMQ_HOME=%~dp0\..\..

:conf
set _WRAPPER_CONF=wrapper.conf


rem
rem Uninstall the Wrapper as an NT service.
rem
:startup
"%_APP_HOME%wrapper.exe" -r %_WRAPPER_CONF%
if not errorlevel 1 goto :eof
pause

