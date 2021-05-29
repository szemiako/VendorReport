$BASE = "C:\ExportFileLog"
$ARCHIVE = "C:\Archive"
$TODAY = [DateTime]::Today.ToString("yyyy.MM.dd")
$TIME_STAMP = Get-Date -Format "yyyyMMdd HHmmss"

function Write-SQL {
    Param($Vendors)
    $sql = "
        SET NOCOUNT ON

        SET QUOTED_IDENTIFIER ON
        GO

        DECLARE @VendorsInScope TABLE (
	        name VARCHAR(100)
        )
    "
    
    foreach ($b in $Vendors.Custodian) {
        $sql += "INSERT INTO @VendorsInScope (name) VALUES ('${b}')`n"
    }

    $sql += "
        SELECT
	        v.id [ID],
	        v.name [VendorName]
        FROM @VendorsInScope vis
        JOIN [Vendors] v WITH (NOLOCK) ON vis.name = v.name  
    "
    $sql | Set-Content "${BASE}GetConfigurations_Query.sql"
}

function Get-IDs {
    Param(
        $Configs,
        $Name,
        $Folder
    )
    cd $BASE
    Write-SQL -Vendors $Configs
    cmd.exe /C "GetConfigurations.bat" "${Name}"
    Remove-Item "${BASE}\GetConfigurations_Query.sql"
    $content = $(Import-Csv ("${Folder}\vendor_ids.csv") -header id, VendorName -delimiter '|')
    Remove-Item "${Folder}\vendor_ids.csv"
    return $content
}

function Send-Report-Email {
    $From = "noreply@saas.com"
    $To = "business@saas.com"
    $Subject = "PRODUCTION: Daily, Outbound Export File Report for ${TODAY}"
    $Body = "<h2>SENT FROM PRODUCTION AT ${TIME_STAMP}</h2>"
    $SMTPServer = "smtp.saas.com"
    $Attachments = (gci -Path ("${BASE}\*.csv"))
    Send-MailMessage -From $From -to $To -Subject $Subject -Body $Body -BodyAsHtml -Attachments $Attachments -SmtpServer $SMTPServer
    Remove-Item $Attachments
}

function Convert-to-Collection {
    Param($Files)
    ## If there are any matches, loop through and add them to object.
    if ($Files) {
        foreach ($f in $Files) {
            $attributes = @{
                vendor_id = $g.id
                VendorName = $g.VendorName
                Status = 'Sent'
                Sent_As = ($f.Name -Replace "${ftp_username}-", "")
                Size = $f.Length
                LastModifiedTime = $f.LastWriteTime
            }
            $results += New-Object PSObject -Property $attributes
        }
    ## If not, write default entry.
    } else {
        $attributes = @{
            vendor_id = $g.id
            VendorName = $g.VendorName
            Status = 'Not Sent'
            Sent_As = ''
            Size = ''
            LastModifiedTime = ''
        }
    $results += New-Object PSObject -Property $attributes
    }
    return $results
}

Foreach ($sub in gci $BASE | Where-Object {$_.PSIsContainer -eq $True}) {
    $Name = $sub.Name
    $AccountFolder = "${BASE}\${Name}"
    [xml]$configs = Get-Content ("${AccountFolder}\Configurations.xml")
    $ftp_username = $configs.configurations.ExportFileString.Name
    
    # Fetch the files.
    ## Figure our which files to fetch.
    $results = @()
    foreach ($g in (Get-IDs -Configs $configs.configurations.VendorsInScope -Name "${Name}" -Folder "${AccountFolder}")) {
        ## Filename mask.
        $mask = ("${ARCHIVE}\${ftp_username}_${TODAY}_" + $g.vendor_id + "_*.zip")
        ## Get the files created today.
        $results = Convert-to-Collection -Files (gci -Path $mask | Where-Object {$_.LastWriteTime -gt (Get-Date).Date})
    }
    $results | export-csv -Path ("${AccountFolder}\_status_${TIME_STAMP}.csv") -NoTypeInformation
}
Send-Report-Email