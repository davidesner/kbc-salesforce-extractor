# Salesforce Writer #

Salesforce Writer component for Keboola Connection.

### Functionality ###

The component takes files from in/tables directory and update appropriate records. Name of object, which will be updated, is taken from the file name (e.g. account.csv will update object Account). CSV file has to have a header with field names, which have to exists in Salesforce. ID column is used to identify record which will be updated.

Would you need to empty values in a field, use #N/A as text value of appropriate records.

The component doesn't create nor delete records in Salesforce.

### Configuration ###
#### Parameters ####

* Loginname - (REQ) your user name, when updating data in sandbox don't forget to add .sandboxname at the end
* Password - (REQ) your password
* Security Token - (REQ) your security token, don't forget it is different for sandbox
* sandbox - (REQ) true when you want to update data in sandbox
