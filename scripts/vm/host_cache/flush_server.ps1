# Small HTTP server on the Windows host that empties its file cache on request from the VM.
# Usage (elevated PowerShell, during the campaign): .\flush_server.ps1
param(
    [string]$Dir = "C:\duckdb-flush",
    [string]$RamMap = "C:\Tools\RAMMap64.exe",
    [int]$PollMs = 100
)

$ErrorActionPreference = "Stop"
if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
          ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run this script in an elevated (Administrator) PowerShell."
}
if (-not (Test-Path $RamMap)) { throw "RAMMap not found at $RamMap" }
$inv = [Globalization.CultureInfo]::InvariantCulture

function Get-CacheMB {
    $m = Get-CimInstance Win32_PerfRawData_PerfOS_Memory
    $standby = [double]$m.StandbyCacheCoreBytes + [double]$m.StandbyCacheNormalPriorityBytes +
               [double]$m.StandbyCacheReserveBytes
    [pscustomobject]@{
        Standby   = [math]::Round($standby / 1MB)
        Modified  = [math]::Round([double]$m.ModifiedPageListBytes / 1MB)
        Available = [math]::Round([double]$m.AvailableBytes / 1MB)
    }
}

New-Item -ItemType Directory -Force -Path $Dir | Out-Null
$log = Join-Path $Dir "flush_log_v2.csv"
if (-not (Test-Path $log)) {
    "timestamp_utc,request,seconds,status,standby_before_MB,standby_after_MB,modified_before_MB,modified_after_MB,available_after_MB" |
        Out-File $log -Encoding ascii
}
& $RamMap -accepteula -Et | Out-Null
$c = Get-CacheMB
Write-Host "Flush server ready on $Dir (standby cache now $($c.Standby) MB; Ctrl+C to stop)"

while ($true) {
    Get-ChildItem -Path $Dir -Filter "req_*" -File -ErrorAction SilentlyContinue | ForEach-Object {
        $id = $_.Name.Substring(4)
        $ack = Join-Path $Dir "ack_$id"
        $before = Get-CacheMB
        $sw = [Diagnostics.Stopwatch]::StartNew()
        try {
            & $RamMap -accepteula -Em | Out-Null
            & $RamMap -accepteula -Et | Out-Null
            $sw.Stop()
            $status = "ok"
        } catch {
            $sw.Stop()
            $status = "error"
            $err = $_.Exception.Message
        }
        $after = Get-CacheMB
        $s = $sw.Elapsed.TotalSeconds.ToString("F3", $inv)
        if ($status -eq "ok") {
            "ok $s $($before.Standby) $($after.Standby)" | Out-File $ack -Encoding ascii
        } else {
            "error $err" | Out-File $ack -Encoding ascii
        }
        Remove-Item $_.FullName -ErrorAction SilentlyContinue
        "{0},{1},{2},{3},{4},{5},{6},{7},{8}" -f (Get-Date).ToUniversalTime().ToString("o"), $id, $s,
            $status, $before.Standby, $after.Standby, $before.Modified, $after.Modified, $after.Available |
            Out-File $log -Append -Encoding ascii
        Write-Host ("{0} flush {1} {2}s  standby {3} -> {4} MB  modified {5} -> {6} MB" -f
            (Get-Date -Format HH:mm:ss), $status, $s, $before.Standby, $after.Standby,
            $before.Modified, $after.Modified)
    }
    Start-Sleep -Milliseconds $PollMs
}
