@echo off

REM ------------------------------------------------------------------------
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.
REM ------------------------------------------------------------------------

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
set ACTIVEMQ_BASE=%~dp0..\..

:conf
set _WRAPPER_CONF="%ACTIVEMQ_HOME%\bin\win32\wrapper.conf"

set _ACTIVEMQ_HOME="set.ACTIVEMQ_HOME=%ACTIVEMQ_HOME%"
set _ACTIVEMQ_BASE="set.ACTIVEMQ_BASE=%ACTIVEMQ_BASE%"

rem
rem Install the Wrapper as an NT service.
rem
:startup
"%_REALPATH%wrapper.exe" -i %_WRAPPER_CONF% %_ACTIVEMQ_HOME% %_ACTIVEMQ_BASE%
if not errorlevel 1 goto :eof
pause

