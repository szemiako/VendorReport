# VendorReport
## Goal
Get a real-time, high-level status report of currently processed vendor feeds using dash, with the ability to drill-down into specific vendors that processed.

Ad-hoc generation of different cross-section of monitoring data for third-party BI tools.
## Requirements
* Read an external JSON file for a list of vendor feeds to expect.
* Query database tables to get processing logs for each vendor feed.
* Poll the server to see export files created.
  * Production records don't exist in database.
  * E.g., Some vendor feeds don't have stored settings.
* Create a report using dash for latest status of feeds.
* Written in:
  * Python
  * SQL
  * Powershell
  * Batch
  * Dash
  * Pandas
## Sample UI
![Sample UI](/sample/sample.png)
