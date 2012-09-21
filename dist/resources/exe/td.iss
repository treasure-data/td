[Setup]
AppName=Treasure Data
AppVersion=<%= version %>
DefaultDirName={pf}\Treasure Data
DefaultGroupName=Treasure Data
Compression=lzma2
SolidCompression=yes
OutputBaseFilename=<%= basename %>
OutputDir=<%= outdir %>
ChangesEnvironment=yes
UsePreviousSetupType=no
AlwaysShowComponentsList=no

; For Ruby expansion ~ 32MB (installed) - 12MB (installer)
ExtraDiskSpaceRequired=20971520

[Types]
Name: client; Description: "Full Installation";
Name: custom; Description: "Custom Installation"; flags: iscustom

[Components]
Name: "toolbelt"; Description: "Treasure Data Toolbelt"; Types: "client custom"
Name: "toolbelt/client"; Description: "Treasure Data Client"; Types: "client custom"; Flags: fixed

[Files]
Source: "td\*.*"; DestDir: "{app}"; Flags: recursesubdirs; Components: "toolbelt/client"
Source: "installers\rubyinstaller.exe"; DestDir: "{tmp}"; Components: "toolbelt/client"

[Icons]
Name: "{group}\Treasure Data command prompt"; Filename: "{app}\td-cmd.bat"

[Registry]
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; ValueType: "expandsz"; ValueName: "Path"; \
  ValueData: "{olddata};{app}\bin"; Check: NeedsAddPath(ExpandConstant('{app}\bin'))
Root: HKCU; Subkey: "Environment"; ValueType: "expandsz"; ValueName: "HOME"; \
  ValueData: "%USERPROFILE%"; Flags: createvalueifdoesntexist

[Run]
Filename: "{tmp}\rubyinstaller.exe"; Parameters: "/verysilent /noreboot /nocancel /noicons /dir=""{app}/ruby-1.9.3"""; \
  Flags: shellexec waituntilterminated; StatusMsg: "Installing Ruby"; Components: "toolbelt/client"
; Filename: "{app}\td-cmd.bat"; Description: "Run command prompt"; Flags: postinstall

[Code]

function NeedsAddPath(Param: string): boolean;
var
  OrigPath: string;
begin
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE,
    'SYSTEM\CurrentControlSet\Control\Session Manager\Environment',
    'Path', OrigPath)
  then begin
    Result := True;
    exit;
  end;
  // look for the path with leading and trailing semicolon
  // Pos() returns 0 if not found
  Result := Pos(';' + Param + ';', ';' + OrigPath + ';') = 0;
end;

function IsProgramInstalled(Name: string): boolean;
var
  ResultCode: integer;
begin
  Result := Exec(Name, 'version', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;
