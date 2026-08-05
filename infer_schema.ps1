param(
    [string[]]$Files = @(Get-ChildItem -Path . -Filter *.csv -Recurse | Select-Object -ExpandProperty FullName)
)

if ($Files.Count -eq 0) {
    Write-Host "No CSV files found in workspace." -ForegroundColor Yellow
    exit 0
}

if (-not (Test-Path -Path './ddls')) { New-Item -ItemType Directory -Path './ddls' | Out-Null }

foreach ($f in $Files) {
    Write-Host "Processing: $f"
    try {
        $sample = Import-Csv -Path $f -Delimiter ',' -Encoding UTF8 | Select-Object -First 1000
    } catch {
        Write-Host ("Failed to read {0}: {1}" -f $f, $_.Exception.Message) -ForegroundColor Red
        continue
    }
    if ($sample.Count -eq 0) { Write-Host "Empty or no rows in $f"; continue }

    $headers = $sample[0].psobject.properties | ForEach-Object { $_.Name }
    $columns = @()

    foreach ($h in $headers) {
        $vals = $sample | ForEach-Object { $_.$h } | Where-Object { $_ -ne $null -and $_ -ne '' }
        if ($vals.Count -eq 0) {
            $type = 'NVARCHAR(4000)'
        } else {
            $allInt = $true; $allFloat = $true; $allDate = $true; $allBool = $true
            $maxlen = 0
            foreach ($v in $vals) {
                $s = [string]$v
                if ($s.Length -gt $maxlen) { $maxlen = $s.Length }
                $intVal = 0; $dblVal = 0.0; $dtVal = [datetime]::MinValue
                if ($allInt -and -not [int]::TryParse($s,[ref]$intVal)) { $allInt = $false }
                if ($allFloat -and -not [double]::TryParse($s,[ref]$dblVal)) { $allFloat = $false }
                if ($allDate -and -not [datetime]::TryParse($s,[ref]$dtVal)) { $allDate = $false }
                $low = $s.ToLower()
                if ($allBool -and -not ($low -in @('true','false','0','1','yes','no'))) { $allBool = $false }
            }
            if ($allInt) { $type = 'BIGINT' }
            elseif ($allFloat) { $type = 'FLOAT' }
            elseif ($allDate) { $type = 'DATETIME2' }
            elseif ($allBool) { $type = 'BIT' }
            else {
                if ($maxlen -le 4000) { $type = "NVARCHAR($maxlen)" } else { $type = 'NVARCHAR(MAX)' }
            }
        }
        $columns += @{name=$h; type=$type}
    }

    $table = [IO.Path]::GetFileNameWithoutExtension($f) -replace '[^a-zA-Z0-9_]','_'
    $ddl = "CREATE SCHEMA IF NOT EXISTS Source_data_0308;`n`n" +
           "CREATE TABLE Source_data_0308.$table (`n"

    $colDefs = @()
    foreach ($c in $columns) { $colDefs += "  [" + $c.name + "] " + $c.type }
    $ddl += ($colDefs -join ",`n")
    $ddl += "`n);"

    $out = Join-Path -Path 'ddls' -ChildPath ($table + '.sql')
    Set-Content -Path $out -Value $ddl -Encoding UTF8
    Write-Host "Wrote: $out" -ForegroundColor Green
}

Write-Host "Done. DDL files are in the ./ddls folder." -ForegroundColor Cyan
