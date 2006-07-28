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

if exist "%HOME%\activemqrc_pre.bat" call "%HOME%\activemqrc_pre.bat"

if "%OS%"=="Windows_NT" @setlocal

rem %~dp0 is expanded pathname of the current script under NT
set DEFAULT_ACTIVEMQ_HOME=%~dp0..

if "%ACTIVEMQ_HOME%"=="" set ACTIVEMQ_HOME=%DEFAULT_ACTIVEMQ_HOME%
set DEFAULT_ACTIVEMQ_HOME=

rem Slurp the command line arguments. This loop allows for an unlimited number
rem of arguments (up to the command line limit, anyway).
set ACTIVEMQ_CMD_LINE_ARGS=%1
if ""%1""=="""" goto doneStart
shift
:setupArgs
if ""%1""=="""" goto doneStart
set ACTIVEMQ_CMD_LINE_ARGS=%ACTIVEMQ_CMD_LINE_ARGS% %1
shift
goto setupArgs
rem This label provides a place for the argument list loop to break out 
rem and for NT handling to skip to.

:doneStart
rem find ACTIVEMQ_HOME if it does not exist due to either an invalid value passed
rem by the user or the %0 problem on Windows 9x
if exist "%ACTIVEMQ_HOME%\README.txt" goto checkJava

rem check for activemq in Program Files on system drive
if not exist "%SystemDrive%\Program Files\activemq" goto checkSystemDrive
set ACTIVEMQ_HOME=%SystemDrive%\Program Files\activemq
goto checkJava

:checkSystemDrive
rem check for activemq in root directory of system drive
if not exist %SystemDrive%\activemq\README.txt goto checkCDrive
set ACTIVEMQ_HOME=%SystemDrive%\activemq
goto checkJava

:checkCDrive
rem check for activemq in C:\activemq for Win9X users
if not exist C:\activemq\README.txt goto noAntHome
set ACTIVEMQ_HOME=C:\activemq
goto checkJava

:noAntHome
echo ACTIVEMQ_HOME is set incorrectly or activemq could not be located. Please set ACTIVEMQ_HOME.
goto end

:checkJava
set _JAVACMD=%JAVACMD%
set LOCALCLASSPATH=%CLASSPATH%

set JAVA_EXT_DIRS=%JAVA_HOME%\lib\ext;%ACTIVEMQ_HOME%;%ACTIVEMQ_HOME%\lib;%ACTIVEMQ_HOME%\lib\optional

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\java.exe
goto runAnt

:noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=java.exe
echo.
echo Warning: JAVA_HOME environment variable is not set.
echo.

:runAnt

if "%ACTIVEMQ_OPTS%" == "" set ACTIVEMQ_OPTS=-Xmx512M -Dderby.system.home="..\data" -Dderby.storage.fileSyncTransactionLog=true

REM Uncomment to enable YourKit profiling
REM SET ACTIVEMQ_DEBUG_OPTS="-Xrunyjpagent"

REM Uncomment to enable remote debugging
REM SET ACTIVEMQ_DEBUG_OPTS=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005

set LOCALCLASSPATH=%ACTIVEMQ_HOME%\conf;%LOCALCLASSPATH%

set ACTIVEMQ_TASK="list"
"%_JAVACMD%" %ACTIVEMQ_DEBUG_OPTS% %ACTIVEMQ_OPTS% -Djava.ext.dirs="%JAVA_EXT_DIRS%" -classpath "%LOCALCLASSPATH%" -jar "%ACTIVEMQ_HOME%/bin/run.jar" %ACTIVEMQ_TASK% %ACTIVEMQ_CMD_LINE_ARGS%


goto end


:end
set LOCALCLASSPATH=
set _JAVACMD=
set ACTIVEMQ_CMD_LINE_ARGS=

if "%OS%"=="Windows_NT" @endlocal

:mainEnd
if exist "%HOME%\activemqrc_post.bat" call "%HOME%\activemqrc_post.bat"


