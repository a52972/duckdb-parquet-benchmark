# Empties the Windows standby list (file cache) once.
# Usage (elevated PowerShell): .\flush_host_cache.ps1
param([string]$RamMap = "C:\Tools\RAMMap64.exe")

$ErrorActionPreference = "Stop"
if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
          ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run this script in an elevated (Administrator) PowerShell."
}
if (-not (Test-Path $RamMap)) { throw "RAMMap not found at $RamMap" }

$sw = [Diagnostics.Stopwatch]::StartNew()
& $RamMap -accepteula -Em | Out-Null
& $RamMap -accepteula -Et | Out-Null
$sw.Stop()
$sw.Elapsed.TotalSeconds.ToString("F3", [Globalization.CultureInfo]::InvariantCulture)
