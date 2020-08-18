echo off
@echo Building with MsBuild

REM Set Path to MSBUILD.exe here

echo Running: "%programfiles(x86)%\MSBuild\14.0\bin\msbuild.exe" build.proj
"%programfiles(x86)%\MSBuild\14.0\bin\msbuild.exe" build.proj


net stop "Service Host Manager Watcher"
net stop "Service Host Manager"
copy .\DecisionsSnowflake.zip "c:\Program Files\Decisions\Decisions Services Manager\CustomModules" /Y
net start "Service Host Manager"
pause
