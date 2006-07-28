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
rem  ============================================
rem  Non-GUI version of JMETER.BAT (WinNT/2K only)
rem
rem  Drop a JMX file on this batch script, and it
rem  will run it in non-GUI mode, with a log file
rem  formed from the input file name but with the
rem  extension .jtl
rem
rem  Only the first parameter is used.
rem  Only works for Win2k.
rem
rem  ============================================

if "%OS%"=="Windows_NT" goto WinNT
echo "Sorry, this command file requires Windows NT/ 2000 / XP"
pause
goto END
:WinNT

rem Change to directory containing this file, which must be in bin
echo Changing to JMeter home directory
cd /D %~dp0

rem Check file is supplied
if a == a%1 goto winNT2
rem Check it has extension .jmx
if a%~x1 == a.jmx goto winNT3
:winNT2
echo Please supply a script name with the extension .jmx
pause
goto :EOF
:winNT3

jmeter -n -t %1 -l %~dpn1.jtl

:END
