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
