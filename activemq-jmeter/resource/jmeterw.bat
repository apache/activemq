@echo off
rem   Run JMeter using javaw

rem   $Id: jmeterw.bat,v 1.4 2005/03/18 15:26:54 mstover1 Exp $
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

set JM_START=start
set JM_LAUNCH=javaw.exe
call jmeter %*
set JM_START=
set JM_LAUNCH=