@ECHO OFF

:: determine if this is x86 or x64
if "%processor_architecture%" == "IA64"  goto x64
if "%processor_architecture%" == "AMD64" goto x64
if "%ProgramFiles%" == "%ProgramW6432%"  goto x64
goto x86

:x86
set TDRubyPath=%ProgramFiles%\ruby-1.9.3
goto launch

:x64
set TDRubyPath=%ProgramFiles(x86)%\ruby-1.9.3
goto launch

:launch

:: determine if this is an NT operating system
if not "%~f0" == "~f0" goto WinNT
goto Win9x

:Win9x
@"%TDRubyPath%\bin\ruby.exe" "td" %1 %2 %3 %4 %5 %6 %7 %8 %9
goto :EOF

:WinNT
@"%TDRubyPath%\bin\ruby.exe" "%~dpn0" %*
goto :EOF
