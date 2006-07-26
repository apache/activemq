@echo off
setlocal

rem Java Service Wrapper general NT service install script


if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
rem
rem Find the application home.
rem
rem %~dp0 is location of current script under NT
set _REALPATH=%~dp0

set ACTIVEMQ_HOME=%~dp0..\..

:conf
set _WRAPPER_CONF=wrapper.conf

set _ACTIVEMQ_HOME="set.ACTIVEMQ_HOME=%ACTIVEMQ_HOME%"

rem
rem Install the Wrapper as an NT service.
rem
:startup
"wrapper.exe" -i %_WRAPPER_CONF% %_ACTIVEMQ_HOME%
if not errorlevel 1 goto :eof
pause

