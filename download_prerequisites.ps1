$downloadsDir = Join-Path $PSScriptRoot "downloads"
if (!(Test-Path $downloadsDir)) {
    New-Item -ItemType Directory -Path $downloadsDir | Out-Null
}

$hadoopUrl = "https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz"
$hadoopFile = Join-Path $downloadsDir "hadoop-3.5.0.tar.gz"

$sparkUrl = "https://dlcdn.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz"
$sparkFile = Join-Path $downloadsDir "spark-4.2.0-bin-hadoop3.tgz"

Write-Host "Checking prerequisites in $downloadsDir..."

if (!(Test-Path $hadoopFile)) {
    Write-Host "Downloading Hadoop 3.5.0 (this only happens once)..."
    curl.exe -L -o $hadoopFile $hadoopUrl
} else {
    Write-Host "Hadoop 3.5.0 archive already present."
}

if (!(Test-Path $sparkFile)) {
    Write-Host "Downloading Spark 4.2.0 (this only happens once)..."
    curl.exe -L -o $sparkFile $sparkUrl
} else {
    Write-Host "Spark 4.2.0 archive already present."
}

Write-Host "Ready to build Docker container: docker compose build"
