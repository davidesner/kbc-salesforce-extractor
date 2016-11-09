package keboola.salesforce.extractor; 

import java.io.*;
import java.util.*;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sun.org.apache.bcel.internal.classfile.Field;

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
	public void main(String[] args) throws AsyncApiException, ConnectionException, IOException   {
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
		BulkConnection connection = getBulkConnection(config.getParams().getLoginname(), config.getParams().getPassword() + config.getParams().getSecuritytoken(), config.getParams().getSandbox());
    	if (connection != null) {
    		List <String> objects = config.getParams().getObject();
    		List <String> soqls = config.getParams().getSOQL();
    		for( int i = 0; i < objects.size(); i++) {
        		sfdown.runQuery( connection, outTablesPath, objects.get(i-1), soqls.get(i-1));	
    		}
    	}
				
   		System.out.println( "All done");
	}

/**
 * If SOQL is empty, generate SELECT * for the object
 */
	public String getSOQL( String soql, String object, BulkConnection connection)
	{
		if( soql != "") { 
	   		System.out.println( "SOQL: " + soql);
	   		return soql; 
	   	}

	    try {
	        // Make the describe call
	    	DescribeSObjectResult describeSObjectResult = connection.describeSObject( object);
		        
	        // Get sObject metadata 
	        if (describeSObjectResult != null) {

		        // Get the fields
		        Field[] fields = describeSObjectResult.getFields();

		        // Iterate through each field and gets its properties 
		        for (int i = 0; i < fields.length; i++) {
		          Field field = fields[i];
		        	  		          
		          // if not formula field publish it
		          if (!field.getType().equals(FieldType.calculated)) {
			          if( soql == ""){
			        	  soql = soql + "," + field.getName();
			          } else {
			        	  soql = field.getName();
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
   		System.out.println( "SOQL: " + soql);

	    return soql;
	}
	
/**
	 * Creates a Bulk API job and uploads batches for a CSV file.
	 */
	public int runQuery( BulkConnection connection, String filesDirectory, String object, String soql)
			throws AsyncApiException, ConnectionException, IOException {
		
		try {
	   		System.out.println( "Processing object: " + object);
			JobInfo job = new JobInfo();
			job.setObject(object);

			job.setOperation(OperationEnum.query);
			job.setConcurrencyMode(ConcurrencyMode.Parallel);
			job.setContentType(ContentType.CSV);

			job = connection.createJob(job);
			assert job.getId() != null;

			job = connection.getJobStatus(job.getId());

			BatchInfo info = null;
			ByteArrayInputStream bout = new ByteArrayInputStream( getSOQL( soql, object, connection).getBytes());
			info = connection.createBatchFromStream(job, bout);

			String[] queryResults = null;

			for(int i=1; i<60000; i++) {
//				Thread.sleep(i==0 ? 30 * 1000 : 30 * 1000); //30 sec
				Thread.sleep(i<30 ? i * 1000 : 30 * 1000); //30 sec
				info = connection.getBatchInfo(job.getId(),	info.getId());

				if (info.getState() == BatchStateEnum.Completed) {
			   		System.out.println( "Completed, getting results.");
					QueryResultList list = connection.getQueryResultList(job.getId(), info.getId());
					queryResults = list.getResult();
					break;
				} else if (info.getState() == BatchStateEnum.Failed) {
					System.err.println("-------------- failed ----------" + info);
					connection.closeJob(job.getId());
					System.exit(1);
				} else {
					System.out.println("-------------- waiting " + ( i < 30 ? i : 30 ) + " seconds ----------"  /* + info */);
				}
			}

			if (queryResults != null) {
				for (String resultId : queryResults) {
					//grabs result stream and passes it to csv writer
			   		System.out.println( "Write everything into " + filesDirectory + object + ".csv");
					FileHandler.writeCSVFromStream(connection.getQueryResultStream(job.getId(),	info.getId(), resultId),object, filesDirectory);
					//grabs results to ensure integrity
					connection.getQueryResultList(job.getId(), info.getId()).getResult();
				}
				//notify user of job complete
				//return number of records complete for data check and close job
				int out = info.getNumberRecordsProcessed();
				connection.closeJob(job.getId());
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


}