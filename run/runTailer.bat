@echo off	

SET mypath=%~dp0
cd %mypath%

for %%a in (%cd%) do (
	set env=%%~nxa
	GOTO:cont
)	
:cont

if not exist ".\Logs\" mkdir .\Logs
if not exist ".\GC_Logs\" mkdir .\GC_Logs
if not exist ".\liveRecordings\" mkdir .\liveRecordings

set ttime=%time: =0%
set now=%date:~-4,4%%date:~-10,2%%date:~-7,2%_%ttime:~0,2%%ttime:~3,2%%ttime:~6,2%
echo %now%
::set args=1603214529527439200 RTAlgorithms_200902_094745
set args=1603214529547093200 RTAlgorithms_200910_020726 14028,569388
set main=chronicle.ChronicleLiveTailer
set JAVA_VER=C:/Program Files/Java/jre1.8.0_261
set outp=output_tailer_%now%
echo %args% > %outp%.log

TITLE %main% %args%
set jarpath=C:/Barak/Jars
echo %date% %time% @ %now% :: The jarpath is "%jarpath%"

set /p jars=<Ver/minjars.txt
set replace=jp
call set jars=%%jars:%replace%^=%jarpath%%%
call set jarsnopath=%%jars:%jarpath%/^=  %%
::echo %jars%

set /a xm=512

"%JAVA_VER%/bin/Java.exe" -Xms%xm%M -Xmx%xm%M  -Dsun.java2d.d3d=true -Dsun.java2d.noddraw=false -Dlog4j.configurationFile=./log4j2.xml -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:GC_Logs/gc.%now%.log -XX:+UseConcMarkSweepGC -classpath "C:/Barak/Jars/*;C:/Barak/Jars/chronicle/*;C:/Users/Mati/IdeaProjects/out/*" %main% %args% > %outp%.log

echo f | xcopy /f /y "%mypath%%outp%.log" "%mypath%Logs\%outp%.log"

set exitCode=%ERRORLEVEL%

setlocal enabledelayedexpansion
:: convert _ to . in folder name
set env=%env:_=.%
:: substring until the dash "-"
for /f "delims=-" %%a in ("%env%") do set env=%%a
endlocal enabledelayedexpansion

pause
exit %exitcode%
