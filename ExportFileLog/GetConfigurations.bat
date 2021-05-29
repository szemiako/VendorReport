echo off
cd %1
sqlcmd -S "PRODUCTION" -d "DB" -h -1 -W -i "C:\Archive\GetConfigurations_Query.sql" -s "|" -o "vendor_ids.csv" 
IF ERRORLEVEL 0 echo SqlCmd operations completed
IF ERRORLEVEL 1 echo SqlCmd operations not completed