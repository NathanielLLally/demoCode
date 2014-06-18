collection of code snippets I keep as samples of my work where making the entire project publically available wouldn't be kosher (or professional for that matter)


ExcelOLE.pm
RTP.pm
json_to_excel.pl
  These three guys automate excel to generate a spreadsheet given data exctracted from MsSQL / apache, then the binary is sent back 


updateFromLoadTables.sql
  An organized way to take a bunch of seperate input data and update corresponding live web tables while performing basic error and sanity checking.  Upon any failure, all relevant data per logical unit will not update and the procedure will continue.

xfer_dfDeal.sql
  This is a gigantic dynamic sql piece that was once a hodge podge of perl hash manipulations deriving fields.  Now it is organized into an xml pseudo language containing aggregate and select statments for pivots followed by the dynamic sql generating code.
