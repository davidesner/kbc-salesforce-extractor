package keboola.salesforce.extractor; 

import java.io.*;
import java.util.*;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

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

   		System.out.println( "Everything ready, let's get some data from Salesforce, loginname: " + config.getParams().getLoginname() + ", SOQL: " + config.getParams().getSOQL());
   		
		Extractor sfdown = new Extractor();
    	sfdown.runQuery(config.getParams().getLoginname(),
				config.getParams().getPassword() + config.getParams().getSecuritytoken(), outTablesPath, config.getParams().getSandbox(), config.getParams().getObject(), config.getParams().getSOQL());
				
   		System.out.println( "All done");
	}


/**
	 * Creates a Bulk API job and uploads batches for a CSV file.
	 */
	public int runQuery(String userName, String password, String filesDirectory, boolean sandbox, String object, String soql)
			throws AsyncApiException, ConnectionException, IOException {
		BulkConnection connection = getBulkConnection(userName, password, sandbox);
		try {
			JobInfo job = new JobInfo();
			job.setObject(object);

			job.setOperation(OperationEnum.query);
			job.setConcurrencyMode(ConcurrencyMode.Parallel);
			job.setContentType(ContentType.CSV);

			job = connection.createJob(job);
			assert job.getId() != null;

			job = connection.getJobStatus(job.getId());

			Calendar time = Calendar.getInstance();
			System.out.println("Query started on object "+object+" at time: "+time.get(Calendar.HOUR_OF_DAY)
			+ ":" + time.get(Calendar.MINUTE)+":"+time.getGreatestMinimum(Calendar.SECOND)+".");   

			BatchInfo info = null;
			ByteArrayInputStream bout = new ByteArrayInputStream(soql.getBytes());
			info = connection.createBatchFromStream(job, bout);

			String[] queryResults = null;

			for(int i=0; i<10000; i++) {
				Thread.sleep(i==0 ? 30 * 1000 : 30 * 1000); //30 sec
				info = connection.getBatchInfo(job.getId(),	info.getId());

				if (info.getState() == BatchStateEnum.Completed) {
					QueryResultList list = connection.getQueryResultList(job.getId(), info.getId());
					queryResults = list.getResult();
					break;
				} else if (info.getState() == BatchStateEnum.Failed) {
					System.out.println("-------------- failed ----------" + info);
					connection.closeJob(job.getId());
					break;
				} else {
					System.out.println("-------------- waiting ----------"/* + info */);
				}
			}

			if (queryResults != null) {
				for (String resultId : queryResults) {
					//grabs result stream and passes it to csv writer
					FileHandler.writeCSVFromStream(connection.getQueryResultStream(job.getId(),	info.getId(), resultId),object, filesDirectory);
					//grabs results to ensure integrity
					connection.getQueryResultList(job.getId(), info.getId()).getResult();
				}
				//notify user of job complete
				Calendar time2 = Calendar.getInstance();
				System.out.println("Output complete on object "+object+" at time: "+time2.get(Calendar.HOUR_OF_DAY)
				+ ":" + time2.get(Calendar.MINUTE)+":"+time2.getGreatestMinimum(Calendar.SECOND));
				//return number of records complete for data check and close job
				int out = info.getNumberRecordsProcessed();
				connection.closeJob(job.getId());
				return out;
			}
		} catch (AsyncApiException aae) {
			System.err.println( aae.printStackTrace());
			System.exit(1);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
			System.err.println("InterruptedExcelption");
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