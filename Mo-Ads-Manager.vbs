Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
appDir = fso.GetParentFolderName(WScript.ScriptFullName) & "\desktop-app"
sh.CurrentDirectory = appDir

If fso.FileExists(appDir & "\node_modules\electron\dist\electron.exe") Then
  sh.Run """" & appDir & "\node_modules\electron\dist\electron.exe"" .", 1, False
Else
  sh.Run """" & appDir & "\Mo-Ads-Manager.bat""", 1, False
End If
