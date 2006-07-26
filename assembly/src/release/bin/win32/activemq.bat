@echo off
rem
rem Find the application home.
rem
if "%OS%"=="Windows_NT" goto nt

echo This is not NT, so please edit this script and set _APP_HOME manually
set _APP_HOME=..

goto conf

:nt
rem %~dp0 is name of current script under NT
set _APP_HOME=%~dp0

rem Set ActiveMQ Home
set ACTIVEMQ_HOME=%~dp0..\..

rem
rem Find the wrapper.conf
rem
:conf
set _WRAPPER_CONF=wrapper.conf

rem
rem Run the application.
rem At runtime, the current directory will be that of Wrapper.exe
rem
"%_APP_HOME%wrapper.exe" -c %_WRAPPER_CONF% 
if not errorlevel 1 goto end
pause

:end
set _APP_HOME=
set _WRAPPER_CONF=