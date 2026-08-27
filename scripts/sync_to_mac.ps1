param(
    [string]$MacUser = 'weifeng',
    [string]$MacHost = '192.168.3.246',
    [string]$MacProject = '~/projects/flex-llm-router'
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$required = @('docs', 'frontend', 'review', 'scripts', 'src', 'templates', 'tests')
$rootFiles = @('.env.example', '.gitignore', 'DESIGN.md', 'README.md', 'pyproject.toml')

if (-not (Test-Path (Join-Path $ProjectRoot '.git') -PathType Container)) {
    throw "Not a Git project: $ProjectRoot"
}
if ((git -C $ProjectRoot status --porcelain).Count -gt 0) {
    throw 'Working tree is not clean; commit local changes before syncing to Mac.'
}

foreach ($name in $rootFiles) {
    $source = Join-Path $ProjectRoot $name
    & scp -o BatchMode=yes -- $source "${MacUser}@${MacHost}:$MacProject/$name"
    if ($LASTEXITCODE -ne 0) { throw "scp failed: $name" }
}

foreach ($name in $required) {
    $source = Join-Path $ProjectRoot $name
    if (-not (Test-Path $source -PathType Container)) { continue }
    & scp -o BatchMode=yes -r -- $source "${MacUser}@${MacHost}:$MacProject/"
    if ($LASTEXITCODE -ne 0) { throw "scp failed: $name" }
}

Write-Output "Synced committed project files to ${MacUser}@${MacHost}:$MacProject"
Write-Output 'Excluded by design: .env, config/ (Mac runtime configuration), data/, logs/, .venv/, .git/, caches, and backups.'
