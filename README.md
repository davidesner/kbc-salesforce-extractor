# Salesforce Writer #

Salesforce Writer component for Keboola Connection.

### Functionality ###

The component takes files from in/tables directory and insert/upsert/update/delete appropriate records. Name of object, which will be updated, is taken from the file name (e.g. account.csv will update object Account). CSV file has to have a header with field names, which have to exists in Salesforce. ID column is used to identify record which will be updated. When inserting records all required fields has to be filled in. 

The files must be in UTF-8 format.

Would you need to empty values in a field, use #N/A as text value of appropriate records.

The process can fail on any records (due to missing required field or too large string) and this specific record will not be inserted/update. Everything else will finish with success. There is no way how to rollback this transaction, so you have to carefully check the log each time. It is also great idea to include a column with external IDs and based on them do upsert later. External IDs will also save you from duplicated records when running insert several times. 

### Configuration ###
#### Parameters ####

* Loginname - (REQ) your user name, when updating data in sandbox don't forget to add .sandboxname at the end
* Password - (REQ) your password
* Security Token - (REQ) your security token, don't forget it is different for sandbox
* sandbox - (REQ) true when you want to update data in sandbox
* upsertField - required when the operation is upsert
* operation - (REQ) specify the operation you wish to do. Insert/Upsert/Update/Delete are supported. 
- when deleting, keep in mind that Salesforce's recycle bin can take less records than you are trying to delete, so they will be hard deleted.
- when upserting the upsertField parameter is required
