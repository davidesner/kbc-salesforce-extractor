package keboola.salesforce.extractor; 

import java.io.*;
import java.util.*;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sun.org.apache.bcel.internal.classfile.Field;
import com.sforce.soap.partner.DescribeSObjectResult; 

import keboola.salesforce.extractor.config.JsonConfigParser;
import keboola.salesforce.extractor.config.KBCConfig;

/**
 *
 * @author David Esner <esnerda at gmail.com>
 * @author Martin Humpolec <martin.humpolec at gmail.com>
 * @created 2016
 */
public class Extractor {
//
	public static void main(String[] args) throws AsyncApiException, ConnectionException, IOException   {
		if (args.length == 0) {
			System.err.println("No parameters provided.");
			System.exit(1);
		}

		String dataPath = args[0];
		String outTablesPath = dataPath + File.separator + "out" + File.separator + "tables" + File.separator;
		
		KBCConfig config = null;
		File confFile = new File(args[0] + File.separator + "config.json");
		if (!confFile.exists()) {
			System.out.println("config.json does not exist!");
			System.err.println("config.json does not exist!");
			System.exit(1);
		}
		// Parse config file
		try {
			if (confFile.exists() && !confFile.isDirectory()) {
				config = JsonConfigParser.parseFile(confFile);
			}
		} catch (Exception ex) {
			System.out.println("Failed to parse config file");
			System.err.println(ex.getMessage());
			System.exit(1);
		}
		if (!config.validate()) {
			System.out.println(config.getValidationError());
			System.err.println(config.getValidationError());
			System.exit(1);
		}

   		System.out.println( "Everything ready, let's get some data from Salesforce, loginname: " + config.getParams().getLoginname());
   		
		Extractor sfdown = new Extractor();
		
		sfdown.runQueries( config.getParams().getLoginname(), config.getParams().getPassword() + config.getParams().getSecuritytoken(), config.getParams().getSandbox(), 
				outTablesPath, config.getParams().getObject(), config.getParams().getSOQL());
				
   		System.out.println( "All done");
	}

/**
 * If SOQL is empty, generate SELECT * for the object
 */
	public String getSOQL( String soql, String object, PartnerConnection connection)
	{
		if( soql == "") { 

		    try {
		        // Make the describe call
		    	DescribeSObjectResult describeSObjectResult = connection.describeSObject( object);
			        
		        // Get sObject metadata 
		        if (describeSObjectResult != null) {

			        // Get the fields
		        	com.sforce.soap.partner.Field[] fields = describeSObjectResult.getFields();

			        // Iterate through each field and gets its properties 
			        for (int i = 0; i < fields.length; i++) {
			        	com.sforce.soap.partner.Field field = fields[i];
//			       		System.out.println( field.getName() + " - " + field.getType());
	 
			       		if( field.getType() != com.sforce.soap.partner.FieldType.address){
			       			
			          // if not formula field publish it
				          if( soql == ""){
				        	  soql = field.getName();
				          } else {
				        	  soql = soql + "," + field.getName();
				          }
				         }
	
				    }
				  }
				} catch (ConnectionException ce) {
				    ce.printStackTrace();
		    }
		    if( soql != "") {
		    	soql = "SELECT " + soql + " FROM " + object;
		    }
		}
   		System.out.println( "SOQL: " + soql);

	    return soql;
	}
	
	/**
	 * Creates a Bulk API job and uploads batches for a CSV file.
	 */
	public int runQueries( String loginname, String password, Boolean sandbox, String filesDirectory, List <String> objects, List <String> soqls)
			throws AsyncApiException, ConnectionException, IOException {
		BulkConnection bulkconnection = getBulkConnection( loginname, password, sandbox);
		PartnerConnection connection = getConnection( loginname, password, sandbox);
    	if (connection != null) {
    		for( int i = 0; i < objects.size(); i++) {
        		System.out.println( "object: " + objects.get(i));
    			String soql = getSOQL( soqls.get(i), objects.get(i), connection);
        		runQuery( bulkconnection, filesDirectory, objects.get(i), soql );	
    		}
    	}
    	return 0;
	}

		/**
	 * Creates a Bulk API job and download batches for a CSV file.
	 */
	public int runQuery( BulkConnection bulkconnection, String filesDirectory, String object, String soql)
			throws AsyncApiException, ConnectionException, IOException {
		
		try {
	   		System.out.println( "Processing object: " + object);
			JobInfo job = new JobInfo();
			job.setObject(object);

			job.setOperation(OperationEnum.query);
			job.setConcurrencyMode(ConcurrencyMode.Parallel);
			job.setContentType(ContentType.CSV);

			job = bulkconnection.createJob(job);
			assert job.getId() != null;

			job = bulkconnection.getJobStatus(job.getId());

			BatchInfo info = null;
			ByteArrayInputStream bout = new ByteArrayInputStream( soql.getBytes());
			info = bulkconnection.createBatchFromStream(job, bout);

			String[] queryResults = null;

			for(int i=1; i<60000; i++) {
//				Thread.sleep(i==0 ? 30 * 1000 : 30 * 1000); //30 sec
				Thread.sleep(i<30 ? i * 1000 : 30 * 1000); //30 sec
				info = bulkconnection.getBatchInfo(job.getId(),	info.getId());

				if (info.getState() == BatchStateEnum.Completed) {
			   		System.out.println( "Completed, getting results.");
					QueryResultList list = bulkconnection.getQueryResultList(job.getId(), info.getId());
					queryResults = list.getResult();
					break;
				} else if (info.getState() == BatchStateEnum.Failed) {
					System.err.println("-------------- failed ----------" + info);
					bulkconnection.closeJob(job.getId());
					System.exit(1);
				} else {
					System.out.println("-------------- waiting " + ( i < 30 ? i : 30 ) + " seconds ----------"  /* + info */);
				}
			}

			if (queryResults != null) {
				for (String resultId : queryResults) {
					//grabs result stream and passes it to csv writer
			   		System.out.println( "Write everything into " + filesDirectory + object + ".csv");
					FileHandler.writeCSVFromStream(bulkconnection.getQueryResultStream(job.getId(),	info.getId(), resultId),object, filesDirectory);
					//grabs results to ensure integrity
					bulkconnection.getQueryResultList(job.getId(), info.getId()).getResult();
				}
				//notify user of job complete
				//return number of records complete for data check and close job
				int out = info.getNumberRecordsProcessed();
				bulkconnection.closeJob(job.getId());
				return out;
			}
		} catch (AsyncApiException aae) {
			aae.printStackTrace();
			System.err.println( "AsyncApiException");
			System.exit(1);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
			System.err.println("InterruptedException");
			System.exit(1);
		}
		//something went wrong here, return 0 to catch an error back in main
		return 0;

		
	}

	/**
	 * Create the BulkConnection used to call Bulk API operations.
	 */
	private BulkConnection getBulkConnection(String userName, String password, boolean sandbox)
			throws ConnectionException, AsyncApiException {
		ConnectorConfig partnerConfig = new ConnectorConfig();
		partnerConfig.setUsername(userName);
		partnerConfig.setPassword(password);
		if ( sandbox == true)  {
			System.out.println("Connecting to Salesforce Sandbox");
			partnerConfig.setAuthEndpoint("https://test.salesforce.com/services/Soap/u/36.0");
		} else {
			System.out.println("Connecting to Salesforce Production");
			partnerConfig.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/36.0");			
		}
		// Creating the connection automatically handles login and stores
		// the session in partnerConfig
		new PartnerConnection(partnerConfig);
		// When PartnerConnection is instantiated, a login is implicitly
		// executed and, if successful,
		// a valid session is stored in the ConnectorConfig instance.
		// Use this key to initialize a BulkConnection:
		ConnectorConfig config = new ConnectorConfig();
		config.setSessionId(partnerConfig.getSessionId());
		// The endpoint for the Bulk API service is the same as for the normal
		// SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
		String soapEndpoint = partnerConfig.getServiceEndpoint();
		String apiVersion = "36.0";
		String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
		config.setRestEndpoint(restEndpoint);
		// This should only be false when doing debugging.
		config.setCompression(true);
		// Set this to true to see HTTP requests and responses on stdout
		config.setTraceMessage(false);
		BulkConnection connection = new BulkConnection(config);
		return connection;
	}

	/**
	 * Create the Connection used to call Describe operations.
	 */
	private PartnerConnection getConnection(String userName, String password, boolean sandbox)
			throws ConnectionException, AsyncApiException {
		ConnectorConfig partnerConfig = new ConnectorConfig();
		partnerConfig.setUsername(userName);
		partnerConfig.setPassword(password);
		if ( sandbox == true)  {
			System.out.println("Connecting to Salesforce Sandbox");
			partnerConfig.setAuthEndpoint("https://test.salesforce.com/services/Soap/u/36.0");
		} else {
			System.out.println("Connecting to Salesforce Production");
			partnerConfig.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/36.0");			
		}
		// Creating the connection automatically handles login and stores
		// the session in partnerConfig
		PartnerConnection connection = new PartnerConnection(partnerConfig);
		return connection;
		// When PartnerConnection is instantiated, a login is implicitly
		// executed and, if successful,
		// a valid session is stored in the ConnectorConfig instance.		
	}

}