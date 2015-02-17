@ECHO OFF

:: determine if this is an NT operating system
if not "%~f0" == "~f0" goto WinNT
goto Win9x

:Win9x
@"%~dp0\..\ruby-1.9.3\bin\ruby.exe" "td" %1 %2 %3 %4 %5 %6 %7 %8 %9
goto :EOF

:WinNT
@"%~dp0\..\ruby-1.9.3\bin\ruby.exe" "%~dpn0" %*
goto :EOF
