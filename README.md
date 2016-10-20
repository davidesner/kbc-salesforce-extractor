# Salesforce Extractor #

Salesforce Extractor component for Keboola Connection.

### Functionality ###

The component export data from Salesforce based on SOQL you provide and save it into the out/tables directory in file named as *object*.csv. 

### Configuration ###
#### Parameters ####

* Loginname - (REQ) your user name, when updating data in sandbox don't forget to add .sandboxname at the end
* Password - (REQ) your password
* Security Token - (REQ) your security token, don't forget it is different for sandbox
* sandbox - (REQ) true when you want to update data in sandbox
* object - (REQ) object from which you want to export data
* soql - (REQ) SOQL to specify which data you want 
