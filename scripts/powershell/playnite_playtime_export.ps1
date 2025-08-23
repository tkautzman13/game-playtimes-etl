# Export playtime data
$current_date = Get-Date
$year = $current_date.ToString("yyyy")
$month = $current_date.ToString("MM")
$day = $current_date.ToString("dd")
$datetime_string = $current_date.ToString("yyyy-MM-dd_HH-mm-ss")

# Build the folder path
$base_path = "D:\Video Game Data Projects\data\raw\playnite\daily_playtime_extracts"
$date_folder = Join-Path -Path $base_path -ChildPath "$year\$month\$day"

# Create the folder structure if it doesn't exist
if (-not (Test-Path -Path $date_folder)) {
    New-Item -Path $date_folder -ItemType Directory -Force
}

# Build the full file path
$path_playtime = Join-Path -Path $date_folder -ChildPath "playnite_playtime_$datetime_string.csv"

# Export the data
$PlayniteApi.Database.Games | Sort-Object -Property Name | Select Name, ID, @{Name='Platforms'; Expr={($_.Platforms | Select-Object -ExpandProperty "Name") -Join ', '}}, @{Name='Categories'; Expr={($_.Categories | Select-Object -ExpandProperty "Name") -Join ', '}}, Hidden, CompletionStatus, LastActivity, Playtime, @{Name='ExportDate'; Expr={$current_date}} | ConvertTo-Csv | Out-File $path_playtime -Encoding utf8