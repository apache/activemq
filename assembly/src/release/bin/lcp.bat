REM   Copyright (c) 2001-2003 The Apache Software Foundation.  All rights
REM   reserved.

set _CLASSPATHCOMPONENT=%1
if ""%1""=="""" goto gotAllArgs
shift

:argCheck
if ""%1""=="""" goto gotAllArgs
set _CLASSPATHCOMPONENT=%_CLASSPATHCOMPONENT% %1
shift
goto argCheck

:gotAllArgs
set LOCALCLASSPATH=%_CLASSPATHCOMPONENT%;%LOCALCLASSPATH%

