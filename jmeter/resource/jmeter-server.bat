@echo off

rem   $Id: jmeter-server.bat,v 1.11 2005/03/18 15:26:54 mstover1 Exp $
rem   Copyright 2001-2004 The Apache Software Foundation
rem 
rem   Licensed under the Apache License, Version 2.0 (the "License");
rem   you may not use this file except in compliance with the License.
rem   You may obtain a copy of the License at
rem 
rem       http://www.apache.org/licenses/LICENSE-2.0
rem 
rem   Unless required by applicable law or agreed to in writing, software
rem   distributed under the License is distributed on an "AS IS" BASIS,
rem   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem   See the License for the specific language governing permissions and
rem   limitations under the License.

REM Protect environment against changes if possible:
if "%OS%"=="Windows_NT" setlocal

rem Need to check if we are using the 4NT shell...
rem [Does that support the ~ constructs?]
if "%eval[2+2]" == "4" goto winNT1
if exist jmeter-server.bat goto winNT1
echo Changing to JMeter home directory
cd /D %~dp0
:winNT1

if exist %JMETER_HOME%\lib\ext\ApacheJMeter_core.jar goto setCP
echo Could not find ApacheJmeter_core.jar ...
REM Try to work out JMETER_HOME
echo ... Trying JMETER_HOME=..
set JMETER_HOME=..
if exist %JMETER_HOME%\lib\ext\ApacheJMeter_core.jar goto setCP
echo ... trying JMETER_HOME=.
set JMETER_HOME=.
if exist %JMETER_HOME%\lib\ext\ApacheJMeter_core.jar goto setCP
echo Cannot determine JMETER_HOME !
goto exit

:setCP
echo Found ApacheJMeter_core.jar
set CLASSPATH=%JMETER_HOME%\lib\ext\ApacheJMeter_core.jar;%JMETER_HOME%\lib\jorphan.jar;%JMETER_HOME%\lib\logkit-1.2.jar
START rmiregistry

if not "%OS%"=="Windows_NT" goto win9xStart
:winNTStart

rem Need to check if we are using the 4NT shell...
if "%eval[2+2]" == "4" goto setup4NT

rem On NT/2K grab all arguments at once
set JMETER_CMD_LINE_ARGS=%*
goto doneStart

:setup4NT
set JMETER_CMD_LINE_ARGS=%$
goto doneStart

:win9xStart
rem Slurp the command line arguments.  This loop allows for an unlimited number of 
rem agruments (up to the command line limit, anyway).

set JMETER_CMD_LINE_ARGS=

:setupArgs
if %1a==a goto doneStart
set JMETER_CMD_LINE_ARGS=%JMETER_CMD_LINE_ARGS% %1
shift
goto setupArgs

:doneStart
rem This label provides a place for the argument list loop to break out 
rem and for NT handling to skip to.

jmeter -s %JMETER_CMD_LINE_ARGS%

:exit