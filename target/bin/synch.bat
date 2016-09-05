@REM ----------------------------------------------------------------------------
@REM Copyright 2001-2004 The Apache Software Foundation.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM ----------------------------------------------------------------------------
@REM

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\repo

set CLASSPATH="%BASEDIR%"\etc;"%REPO%"\xml-apis\xmlParserAPIs\2.0.2\xmlParserAPIs-2.0.2.jar;"%REPO%"\javax\xml\stream\stax-api\1.0-2\stax-api-1.0-2.jar;"%REPO%"\com\force\api\force-wsc\37.0.3\force-wsc-37.0.3.jar;"%REPO%"\org\antlr\ST4\4.0.7\ST4-4.0.7.jar;"%REPO%"\org\antlr\antlr-runtime\3.5\antlr-runtime-3.5.jar;"%REPO%"\org\antlr\stringtemplate\3.2.1\stringtemplate-3.2.1.jar;"%REPO%"\antlr\antlr\2.7.7\antlr-2.7.7.jar;"%REPO%"\org\codehaus\jackson\jackson-core-asl\1.9.13\jackson-core-asl-1.9.13.jar;"%REPO%"\org\codehaus\jackson\jackson-mapper-asl\1.9.13\jackson-mapper-asl-1.9.13.jar;"%REPO%"\commons-beanutils\commons-beanutils\1.7.0\commons-beanutils-1.7.0.jar;"%REPO%"\commons-logging\commons-logging\1.0.3\commons-logging-1.0.3.jar;"%REPO%"\com\heroku\sdk\heroku-jdbc\0.1.1\heroku-jdbc-0.1.1.jar;"%REPO%"\org\stand\enterprise\2.0\enterprise-2.0.jar;"%REPO%"\org\stand\partner\2.0\partner-2.0.jar;"%REPO%"\com\sparkjava\spark-core\2.2\spark-core-2.2.jar;"%REPO%"\org\slf4j\slf4j-api\1.7.7\slf4j-api-1.7.7.jar;"%REPO%"\org\slf4j\slf4j-simple\1.7.7\slf4j-simple-1.7.7.jar;"%REPO%"\org\eclipse\jetty\jetty-server\9.0.2.v20130417\jetty-server-9.0.2.v20130417.jar;"%REPO%"\org\eclipse\jetty\orbit\javax.servlet\3.0.0.v201112011016\javax.servlet-3.0.0.v201112011016.jar;"%REPO%"\org\eclipse\jetty\jetty-webapp\9.0.2.v20130417\jetty-webapp-9.0.2.v20130417.jar;"%REPO%"\org\eclipse\jetty\jetty-xml\9.0.2.v20130417\jetty-xml-9.0.2.v20130417.jar;"%REPO%"\org\eclipse\jetty\jetty-servlet\9.0.2.v20130417\jetty-servlet-9.0.2.v20130417.jar;"%REPO%"\org\eclipse\jetty\jetty-security\9.0.2.v20130417\jetty-security-9.0.2.v20130417.jar;"%REPO%"\com\sparkjava\spark-template-freemarker\2.0.0\spark-template-freemarker-2.0.0.jar;"%REPO%"\org\freemarker\freemarker\2.3.19\freemarker-2.3.19.jar;"%REPO%"\org\postgresql\postgresql\9.4-1201-jdbc4\postgresql-9.4-1201-jdbc4.jar;"%REPO%"\org\cometd\java\bayeux-api\2.3.1\bayeux-api-2.3.1.jar;"%REPO%"\org\cometd\java\cometd-java-client\2.3.1\cometd-java-client-2.3.1.jar;"%REPO%"\org\cometd\java\cometd-java-common\2.3.1\cometd-java-common-2.3.1.jar;"%REPO%"\org\eclipse\jetty\jetty-client\7.4.4.v20110707\jetty-client-7.4.4.v20110707.jar;"%REPO%"\org\eclipse\jetty\jetty-util\7.4.4.v20110707\jetty-util-7.4.4.v20110707.jar;"%REPO%"\org\eclipse\jetty\jetty-http\7.4.4.v20110707\jetty-http-7.4.4.v20110707.jar;"%REPO%"\org\eclipse\jetty\jetty-io\7.4.4.v20110707\jetty-io-7.4.4.v20110707.jar;"%REPO%"\org\json\json\20151123\json-20151123.jar;"%REPO%"\commons-lang\commons-lang\2.3\commons-lang-2.3.jar;"%REPO%"\redis\clients\jedis\2.8.0\jedis-2.8.0.jar;"%REPO%"\org\apache\commons\commons-pool2\2.3\commons-pool2-2.3.jar;"%REPO%"\com\sendgrid\sendgrid-java\2.2.2\sendgrid-java-2.2.2.jar;"%REPO%"\org\apache\httpcomponents\httpcore\4.3.2\httpcore-4.3.2.jar;"%REPO%"\org\apache\httpcomponents\httpmime\4.3.4\httpmime-4.3.4.jar;"%REPO%"\com\sendgrid\smtpapi-java\1.2.0\smtpapi-java-1.2.0.jar;"%REPO%"\org\apache\httpcomponents\httpclient\4.3.4\httpclient-4.3.4.jar;"%REPO%"\commons-codec\commons-codec\1.6\commons-codec-1.6.jar;"%REPO%"\org\apache\commons\commons-collections4\4.0\commons-collections4-4.0.jar;"%REPO%"\org\example\standsynchscore\2.0\standsynchscore-2.0.jar
set EXTRA_JVM_ARGUMENTS=
goto endInit

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS% %EXTRA_JVM_ARGUMENTS% -classpath %CLASSPATH_PREFIX%;%CLASSPATH% -Dapp.name="synch" -Dapp.repo="%REPO%" -Dbasedir="%BASEDIR%" CacheSynch %CMD_LINE_ARGS%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@endlocal

:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%
