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

if "%ACTIVEMQ_BASE%" == "" set ACTIVEMQ_BASE=%ACTIVEMQ_HOME%

if "%ACTIVEMQ_CONF%" == "" set ACTIVEMQ_CONF=%ACTIVEMQ_HOME%\conf

if "%ACTIVEMQ_DATA%" == "" set ACTIVEMQ_DATA=%ACTIVEMQ_HOME%\data

if "%ACTIVEMQ_TMP%" == "" set ACTIVEMQ_TMP=%ACTIVEMQ_DATA%\tmp

if /i not "%1" == "start" goto debugOpts


if "%ACTIVEMQ_OPTS%" == "" set ACTIVEMQ_OPTS=-Xmx1G -Dorg.apache.activemq.UseDedicatedTaskRunner=true -Djava.util.logging.config.file=logging.properties -Djava.security.auth.login.config=%ACTIVEMQ_CONF%\login.config

if "%ACTIVEMQ_SUNJMX_START%" == "" set ACTIVEMQ_SUNJMX_START=-Dcom.sun.management.jmxremote
REM set ACTIVEMQ_SUNJMX_START=-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false


:debugOpts
REM Uncomment to enable YourKit profiling
REM SET ACTIVEMQ_DEBUG_OPTS="-agentlib:yjpagent"

REM Uncomment to enable remote debugging
REM SET ACTIVEMQ_DEBUG_OPTS=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005

REM Setup ActiveMQ Classpath. Default is the conf directory.
set ACTIVEMQ_CLASSPATH=%ACTIVEMQ_CONF%;%ACTIVEMQ_DATA%;%ACTIVEMQ_CLASSPATH%
"%_JAVACMD%" %ACTIVEMQ_SUNJMX_START% %ACTIVEMQ_DEBUG_OPTS% %ACTIVEMQ_OPTS% %ACTIVEMQ_SSL_OPTS% -Dactivemq.classpath="%ACTIVEMQ_CLASSPATH%" -Dactivemq.home="%ACTIVEMQ_HOME%" -Dactivemq.base="%ACTIVEMQ_BASE%" -Dactivemq.data="%ACTIVEMQ_DATA%" -Djava.io.tmpdir="%ACTIVEMQ_TMP%" -Dactivemq.conf="%ACTIVEMQ_CONF%" -jar "%ACTIVEMQ_HOME%/bin/activemq.jar" %*

goto end


:end
set _JAVACMD=

if "%OS%"=="Windows_NT" @endlocal

:mainEnd
if exist "%HOME%\activemqrc_post.bat" call "%HOME%\activemqrc_post.bat"


