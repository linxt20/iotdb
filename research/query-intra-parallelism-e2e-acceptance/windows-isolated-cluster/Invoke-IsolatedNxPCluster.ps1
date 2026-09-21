# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may obtain a copy of the
# License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

<#
.SYNOPSIS
Creates, starts, stops, and diagnoses a local Windows-only pair of isolated 1C2D IoTDB clusters.

.DESCRIPTION
The deployment consists of a hash-enabled candidate and an otherwise equivalent hash-disabled
control.  It is deliberately intended for the multi-source N x P GROUP BY acceptance runner.
Every node has an explicit, non-default port set and an independent configuration, data, log, and
process manifest below Root.  The IoTDB distribution supplied to Prepare is never modified.

Prepare refuses to use a non-empty Root. Start checks all assigned listen ports before launching
anything. Stop terminates only PIDs recorded in this deployment's manifest, after confirming that
the process command line still contains this deployment's absolute root.  It never searches for or
stops a generic IoTDB or Java process.
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('Prepare', 'Start', 'Stop', 'Status')]
  [string]$Mode,

  [Parameter(Mandatory = $true)]
  [string]$Root,

  [string]$DistributionRoot,

  [ValidateSet('Candidate', 'Control', 'Both')]
  [string]$Deployment = 'Both',

  [int]$StartupTimeoutSeconds = 120,

  [switch]$KeepRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ManifestName = 'isolation-manifest.json'
$PropertySourceName = 'iotdb-system.properties'
$ConfigNodeClass = 'org.apache.iotdb.confignode.service.ConfigNode'
$DataNodeClass = 'org.apache.iotdb.db.service.DataNode'

function Get-AbsolutePath([string]$Path) {
  return [System.IO.Path]::GetFullPath($Path)
}

function Assert-Distribution([string]$Path) {
  $resolved = Get-AbsolutePath $Path
  $required = @(
    (Join-Path $resolved 'lib'),
    (Join-Path $resolved 'conf\iotdb-system.properties'),
    (Join-Path $resolved 'conf\logback-confignode.xml'),
    (Join-Path $resolved 'conf\logback-datanode.xml')
  )
  $missing = @($required | Where-Object { -not (Test-Path -LiteralPath $_) })
  if ($missing.Count -gt 0) {
    throw "DistributionRoot is not a built all-bin distribution; missing: $($missing -join ', ')"
  }
  if (@(Get-ChildItem -LiteralPath (Join-Path $resolved 'lib') -Filter '*.jar').Count -eq 0) {
    throw "DistributionRoot has no runtime JARs: $resolved"
  }
  return $resolved
}

function Get-NodeDefinitions {
  # Non-default port blocks make this safe to run next to any conventional 10710/6667 deployment.
  return @(
    [ordered]@{ deployment = 'candidate'; name = 'confignode'; role = 'ConfigNode'; rpc = $null; internal = 21010; consensus = 21020; mpp = $null; schema = $null; dataConsensus = $null; metric = 21901 },
    [ordered]@{ deployment = 'candidate'; name = 'datanode-1'; role = 'DataNode'; rpc = 21667; internal = 21030; consensus = $null; mpp = 21040; schema = 21050; dataConsensus = 21060; metric = 21902 },
    [ordered]@{ deployment = 'candidate'; name = 'datanode-2'; role = 'DataNode'; rpc = 22667; internal = 22030; consensus = $null; mpp = 22040; schema = 22050; dataConsensus = 22060; metric = 22902 },
    [ordered]@{ deployment = 'control'; name = 'confignode'; role = 'ConfigNode'; rpc = $null; internal = 31010; consensus = 31020; mpp = $null; schema = $null; dataConsensus = $null; metric = 31901 },
    [ordered]@{ deployment = 'control'; name = 'datanode-1'; role = 'DataNode'; rpc = 31667; internal = 31030; consensus = $null; mpp = 31040; schema = 31050; dataConsensus = 31060; metric = 31902 },
    [ordered]@{ deployment = 'control'; name = 'datanode-2'; role = 'DataNode'; rpc = 32667; internal = 32030; consensus = $null; mpp = 32040; schema = 32050; dataConsensus = 32060; metric = 32902 }
  )
}

function Get-Ports([object]$Node) {
  return @($Node.rpc, $Node.internal, $Node.consensus, $Node.mpp, $Node.schema, $Node.dataConsensus, $Node.metric | Where-Object { $null -ne $_ })
}

function Test-PortAvailable([int]$Port) {
  $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)
  try {
    $listener.Start()
    return $true
  } catch [System.Net.Sockets.SocketException] {
    return $false
  } finally {
    $listener.Stop()
  }
}

function Assert-PortsAvailable([object[]]$Nodes) {
  $ports = @($Nodes | ForEach-Object { Get-Ports $_ } | Sort-Object -Unique)
  $used = @($ports | Where-Object { -not (Test-PortAvailable $_) })
  if ($used.Count -gt 0) {
    throw "Refusing to start: assigned isolated ports are already occupied: $($used -join ', ')"
  }
}

function Copy-NodeConf([string]$Distribution, [string]$NodeRoot, [System.Collections.IDictionary]$Node, [System.Collections.IDictionary]$ConfigNode, [string]$ClusterName) {
  $conf = Join-Path $NodeRoot 'conf'
  $logs = Join-Path $NodeRoot 'logs'
  $data = Join-Path $NodeRoot 'data'
  foreach ($directory in @($conf, $logs, $data)) {
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
  }
  Copy-Item -LiteralPath (Join-Path $Distribution 'conf\logback-confignode.xml') -Destination $conf
  Copy-Item -LiteralPath (Join-Path $Distribution 'conf\logback-datanode.xml') -Destination $conf
  $enabled = if ($Node.deployment -eq 'candidate') { 'true' } else { 'false' }
  # ConfigNode parses the common file too. Keep every DataNode numeric setting syntactically valid
  # there by borrowing its deployment's first DataNode port block; ConfigNode does not bind them.
  $dataNode = if ($Node.role -eq 'ConfigNode') {
    if ($Node.deployment -eq 'candidate') {
      [ordered]@{ rpc = 21667; internal = 21030; mpp = 21040; schema = 21050; dataConsensus = 21060; metric = 21902 }
    } else {
      [ordered]@{ rpc = 31667; internal = 31030; mpp = 31040; schema = 31050; dataConsensus = 31060; metric = 31902 }
    }
  } else {
    $Node
  }
  $properties = @(
    '# Generated by Invoke-IsolatedNxPCluster.ps1. Do not reuse outside this isolated deployment.',
    "cluster_name=$ClusterName",
    "cn_seed_config_node=127.0.0.1:$($ConfigNode.internal)",
    "dn_seed_config_node=127.0.0.1:$($ConfigNode.internal)",
    'schema_replication_factor=1',
    'data_replication_factor=1',
    'degree_of_query_parallelism=4',
    'enable_dop_estimation=false',
    "enable_property_driven_planning=$enabled",
    "enable_table_group_by_hash_repartition=$enabled",
    'table_group_by_hash_repartition_partition_count=2',
    'cn_internal_address=127.0.0.1',
    "cn_internal_port=$($ConfigNode.internal)",
    "cn_consensus_port=$($ConfigNode.consensus)",
    "cn_system_dir=$(Join-Path $data 'confignode-system')",
    "cn_consensus_dir=$(Join-Path $data 'confignode-consensus')",
    "cn_pipe_receiver_file_dir=$(Join-Path $data 'confignode-pipe-receiver')",
    'dn_rpc_address=127.0.0.1',
    "dn_rpc_port=$($dataNode.rpc)",
    'dn_internal_address=127.0.0.1',
    "dn_internal_port=$($dataNode.internal)",
    "dn_mpp_data_exchange_port=$($dataNode.mpp)",
    "dn_schema_region_consensus_port=$($dataNode.schema)",
    "dn_data_region_consensus_port=$($dataNode.dataConsensus)",
    "dn_system_dir=$(Join-Path $data 'datanode-system')",
    "dn_data_dirs=$(Join-Path $data 'datanode-data')",
    "dn_consensus_dir=$(Join-Path $data 'datanode-consensus')",
    "dn_wal_dirs=$(Join-Path $data 'datanode-wal')",
    "dn_tracing_dir=$(Join-Path $data 'datanode-tracing')",
    "dn_sync_dir=$(Join-Path $data 'datanode-sync')",
    "dn_pipe_receiver_file_dirs=$(Join-Path $data 'datanode-pipe-receiver')",
    'cn_metric_reporter_list=',
    "cn_metric_prometheus_reporter_port=$($Node.metric)",
    'dn_metric_reporter_list=',
    "dn_metric_prometheus_reporter_port=$($dataNode.metric)"
  )
  Set-Content -LiteralPath (Join-Path $conf $PropertySourceName) -Value $properties -Encoding utf8
}

function Read-Manifest([string]$DeploymentRoot) {
  $path = Join-Path $DeploymentRoot $ManifestName
  if (-not (Test-Path -LiteralPath $path)) {
    throw "No isolated deployment manifest exists at $path. Run -Mode Prepare first."
  }
  return Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
}

function Get-ClassPath([string]$Distribution) {
  return ((Get-ChildItem -LiteralPath (Join-Path $Distribution 'lib') -Filter '*.jar' | ForEach-Object FullName) -join ';')
}

function Wait-ForPort([int]$Port, [int]$TimeoutSeconds, [string]$NodeName) {
  $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
  do {
    if (-not (Test-PortAvailable $Port)) { return }
    Start-Sleep -Milliseconds 500
  } while ([DateTime]::UtcNow -lt $deadline)
  throw "Timed out waiting for $NodeName to listen on port $Port"
}

function Start-Node([pscustomobject]$Node, [pscustomobject]$Manifest, [string]$ClassPath, [int]$TimeoutSeconds) {
  $root = [string]$Node.root
  $conf = Join-Path $root 'conf'
  $logs = Join-Path $root 'logs'
  $data = Join-Path $root 'data'
  $role = [string]$Node.role
  $class = if ($role -eq 'ConfigNode') { $ConfigNodeClass } else { $DataNodeClass }
  $logConfig = Join-Path $conf (if ($role -eq 'ConfigNode') { 'logback-confignode.xml' } else { 'logback-datanode.xml' })
  $properties = @(
    "-Dlogback.configurationFile=$logConfig",
    "-D$($role.ToUpperInvariant())_HOME=$($Manifest.distribution_root)",
    "-D$($role.ToUpperInvariant())_DATA_HOME=$data",
    "-D$($role.ToUpperInvariant())_CONF=$conf",
    "-DTSFILE_HOME=$($Manifest.distribution_root)",
    "-DTSFILE_CONF=$conf",
    "-D$($role.ToUpperInvariant())_LOGS=$logs",
    "-D$($role.ToUpperInvariant())_LOG_DIR=$logs",
    '-Diotdb-foreground=yes',
    '-Xms256m', '-Xmx512m', '-cp', $ClassPath, $class, '-s'
  )
  $stdout = Join-Path $logs 'stdout.log'
  $stderr = Join-Path $logs 'stderr.log'
  $process = Start-Process -FilePath 'java' -ArgumentList $properties -WorkingDirectory $root -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
  $pidPath = Join-Path $root 'process.json'
  [ordered]@{ pid = $process.Id; class = $class; started_at_utc = [DateTime]::UtcNow.ToString('o'); root = $root } |
    ConvertTo-Json | Set-Content -LiteralPath $pidPath -Encoding utf8
  $port = if ($role -eq 'ConfigNode') { [int]$Node.internal } else { [int]$Node.rpc }
  try {
    Wait-ForPort $port $TimeoutSeconds $Node.name
  } catch {
    if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
    throw "$($_.Exception.Message). Inspect $stderr and $stdout"
  }
}

function Stop-Node([pscustomobject]$Node, [string]$ExpectedRoot) {
  $processPath = Join-Path ([string]$Node.root) 'process.json'
  if (-not (Test-Path -LiteralPath $processPath)) { return [ordered]@{ node = $Node.name; stopped = $false; reason = 'no-process-manifest' } }
  $record = Get-Content -LiteralPath $processPath -Raw | ConvertFrom-Json
  $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($record.pid)" -ErrorAction SilentlyContinue
  if ($null -eq $process) { return [ordered]@{ node = $Node.name; stopped = $false; reason = 'pid-not-running' } }
  if ($process.CommandLine -notlike "*$ExpectedRoot*") {
    throw "Refusing to stop PID $($record.pid): command line no longer identifies this isolated root."
  }
  Stop-Process -Id $record.pid -Force
  return [ordered]@{ node = $Node.name; stopped = $true; pid = $record.pid }
}

$deploymentRoot = Get-AbsolutePath $Root
if ($Mode -eq 'Prepare') {
  if (-not $DistributionRoot) { throw '-DistributionRoot is required for -Mode Prepare.' }
  $distribution = Assert-Distribution $DistributionRoot
  if ((Test-Path -LiteralPath $deploymentRoot) -and @(Get-ChildItem -Force -LiteralPath $deploymentRoot).Count -gt 0) {
    throw "Prepare requires a new or empty Root. Refusing to overwrite $deploymentRoot"
  }
  $nodes = Get-NodeDefinitions
  Assert-PortsAvailable $nodes
  if ($PSCmdlet.ShouldProcess($deploymentRoot, 'create isolated candidate/control deployment')) {
    New-Item -ItemType Directory -Force -Path $deploymentRoot | Out-Null
    foreach ($node in $nodes) {
      $nodeRoot = Join-Path $deploymentRoot "$($node.deployment)\$($node.name)"
      $config = $nodes | Where-Object { $_.deployment -eq $node.deployment -and $_.role -eq 'ConfigNode' } | Select-Object -First 1
      Copy-NodeConf $distribution $nodeRoot $node $config "isolated-nxp-$($node.deployment)"
      $node.root = $nodeRoot
    }
    $manifest = [ordered]@{
      schema_version = 1
      purpose = 'local Windows isolated multi-DataNode N x P GROUP BY acceptance only'
      created_at_utc = [DateTime]::UtcNow.ToString('o')
      deployment_root = $deploymentRoot
      distribution_root = $distribution
      distribution_jar_count = @(Get-ChildItem -LiteralPath (Join-Path $distribution 'lib') -Filter '*.jar').Count
      nodes = $nodes
    }
    $manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $deploymentRoot $ManifestName) -Encoding utf8
    $manifest | ConvertTo-Json -Depth 5
  }
  exit 0
}

$manifest = Read-Manifest $deploymentRoot
if ((Get-AbsolutePath ([string]$manifest.deployment_root)) -ne $deploymentRoot) { throw 'Manifest root does not match the requested Root.' }
$selected = @($manifest.nodes | Where-Object { $Deployment -eq 'Both' -or $_.deployment -eq $Deployment.ToLowerInvariant() })
if ($selected.Count -eq 0) { throw "No nodes selected for deployment $Deployment" }

if ($Mode -eq 'Start') {
  Assert-Distribution ([string]$manifest.distribution_root) | Out-Null
  Assert-PortsAvailable $selected
  $classPath = Get-ClassPath ([string]$manifest.distribution_root)
  foreach ($node in @($selected | Where-Object role -eq 'ConfigNode')) { Start-Node $node $manifest $classPath $StartupTimeoutSeconds }
  foreach ($node in @($selected | Where-Object role -eq 'DataNode')) { Start-Node $node $manifest $classPath $StartupTimeoutSeconds }
  if (-not $KeepRunning) {
    Write-Output 'Nodes started. Run -Mode Status for evidence, then -Mode Stop before deleting this isolated root.'
  }
  exit 0
}

if ($Mode -eq 'Stop') {
  $outcomes = foreach ($node in @($selected | Sort-Object { if ($_.role -eq 'DataNode') { 0 } else { 1 } })) {
    Stop-Node $node $deploymentRoot
  }
  $outcomes | ConvertTo-Json
  exit 0
}

$status = foreach ($node in $selected) {
  $processPath = Join-Path ([string]$node.root) 'process.json'
  $pid = $null
  $running = $false
  if (Test-Path -LiteralPath $processPath) {
    $pid = (Get-Content -LiteralPath $processPath -Raw | ConvertFrom-Json).pid
    $running = $null -ne (Get-Process -Id $pid -ErrorAction SilentlyContinue)
  }
  [ordered]@{ deployment = $node.deployment; node = $node.name; role = $node.role; pid = $pid; process_running = $running; expected_listen_port = if ($node.role -eq 'ConfigNode') { $node.internal } else { $node.rpc }; stderr = (Join-Path ([string]$node.root) 'logs\stderr.log') }
}
$status | ConvertTo-Json
