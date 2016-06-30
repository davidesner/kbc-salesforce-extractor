package keboola.salesforce.writer;

import java.io.*;
import java.util.*;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import keboola.salesforce.writer.config.JsonConfigParser;
import keboola.salesforce.writer.config.KBCConfig;

/**
 *
 * @author David Esner <esnerda at gmail.com>
 * @author Martin Humpolec <martin.humpolec at gmail.com>
 * @created 2016
 */
public class Writer {
//
	public static void main(String[] args) throws AsyncApiException, ConnectionException, IOException   {
		if (args.length == 0) {
			System.out.print("No parameters provided.");
			System.exit(1);
		}

		String dataPath = args[0];
		String inTablesPath = dataPath + File.separator + "in" + File.separator + "tables" + File.separator;
		
    	System.out.println("looking for config");
		
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

   		System.out.println( "Everything ready, write to Salesforce, loginname: " + config.getParams().getLoginname());
		Writer sfupd = new Writer();
		sfupd.runUpdate(config.getParams().getLoginname(),
				config.getParams().getPassword() + config.getParams().getSecuritytoken(), inTablesPath, config.getParams().getSandbox());
				
   		System.out.println( "All done");
	}


/**
	 * Creates a Bulk API job and uploads batches for a CSV file.
	 */
	public void runUpdate(String userName, String password, String filesDirectory, boolean sandbox)
			throws AsyncApiException, ConnectionException, IOException {
   		System.out.println( "runUpdate start");

		BulkConnection connection = getBulkConnection(userName, password, sandbox);
		
		File folder = new File( filesDirectory);
		File[] listOfFiles = folder.listFiles();

    	for (int i = 0; i < listOfFiles.length; i++) {
      	if (listOfFiles[i].isFile()) {
			 String fileName = listOfFiles[i].getName().toString();
			 int position = fileName.indexOf('.');
			 String fileNameShort = fileName.substring( 0, position);
			 boolean manifest = fileName.endsWith( "manifest");
			 if( manifest == false) {
				System.out.println( "found file " + fileName + ", object " + fileNameShort);
				JobInfo job = createJob( fileNameShort, connection);
				List<BatchInfo> batchInfoList = createBatchesFromCSVFile(connection, job, filesDirectory + listOfFiles[i].getName());
				closeJob(connection, job.getId());
				awaitCompletion(connection, job, batchInfoList);
				checkResults(connection, job, batchInfoList);
			}
      	  }
    	}
		
   		System.out.println( "runUpdate end");
	}

	/**
	 * Gets the results of the operation and checks for errors.
	 */
	private void checkResults(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList)
			throws AsyncApiException, IOException {
		// batchInfoList was populated when batches were created and submitted
   		System.out.println( "checkResult start");
		for (BatchInfo b : batchInfoList) {
	   		System.out.println( "checkResult CSV reader");
			CSVReader rdr = new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
			List<String> resultHeader = rdr.nextRecord();
			int resultCols = resultHeader.size();
	   		System.out.println( "checkResult size: " + resultCols);

			List<String> row;
			while ((row = rdr.nextRecord()) != null) {
		   		System.out.println( "checkResult Map");
				Map<String, String> resultInfo = new HashMap<String, String>();
				for (int i = 0; i < resultCols; i++) {
			   		System.out.println( "checkResult Adding results");
					resultInfo.put(resultHeader.get(i), row.get(i));
				}
				boolean success = Boolean.valueOf(resultInfo.get("Success"));
				boolean created = Boolean.valueOf(resultInfo.get("Created"));
				String id = resultInfo.get("Id");
				String error = resultInfo.get("Error");
		   		System.out.println( "checkResult errors?");

				if (success && created) {
					System.out.println("Updated row with id " + id);
				} else if (!success) {
					System.out.println("Failed with error: " + error);
				}
			}
		}
	}

	private void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
   		System.out.println( "closeJob start");
		JobInfo job = new JobInfo();
		job.setId(jobId);
		job.setState(JobStateEnum.Closed);
		connection.updateJob(job);
   		System.out.println( "closeJob end");
	}

	/**
	 * Wait for a job to complete by polling the Bulk API.
	 * 
	 * @param connection
	 *            BulkConnection used to check results.
	 * @param job
	 *            The job awaiting completion.
	 * @param batchInfoList
	 *            List of batches for this job.
	 * @throws AsyncApiException
	 */
	private void awaitCompletion(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList)
			throws AsyncApiException {
		long sleepTime = 0L;
		Set<String> incomplete = new HashSet<String>();
		for (BatchInfo bi : batchInfoList) {
			incomplete.add(bi.getId());
		}
		while (!incomplete.isEmpty()) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
			}
			System.out.println("Awaiting results..." + incomplete.size());
			sleepTime = 10000L;
			try {
				BatchInfo[] statusList = connection.getBatchInfoList(job.getId()).getBatchInfo();
				for (BatchInfo b : statusList) {
					if (b.getState() == BatchStateEnum.Completed || b.getState() == BatchStateEnum.Failed) {
						if (incomplete.remove(b.getId())) {
							System.out.println("BATCH STATUS:\n" + b);
						}
					}
				}
			} catch ( Exception e) {
				System.err.println( "awaitCompletion error " + e.getMessage() );
			}
		}
	}

	/**
	 * Create a new job using the Bulk API.
	 * 
	 * @param sobjectType
	 *            The object type being loaded, such as "Account"
	 * @param connection
	 *            BulkConnection used to create the new job.
	 * @return The JobInfo for the new job.
	 * @throws AsyncApiException
	 */
	private JobInfo createJob(String sobjectType, BulkConnection connection) throws AsyncApiException {
   		System.out.println( "createJob start, object: " + sobjectType);
		try {
			JobInfo job = new JobInfo();
			job.setObject(sobjectType);
			job.setOperation(OperationEnum.update);
			job.setContentType(ContentType.CSV);
			job = connection.createJob(job);
			System.out.println(job);
			System.out.println( "createJob end");
			return job;
		}
		catch ( Exception e) {
			System.err.println( "createJob error " + e.getMessage() );
			return null;
		}
	}

	/**
	 * Create the BulkConnection used to call Bulk API operations.
	 */
	private BulkConnection getBulkConnection(String userName, String password, boolean sandbox)
			throws ConnectionException, AsyncApiException {
   		System.out.println( "getBulkConnection start");
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
   		System.out.println( "getBulkConnection end");
		return connection;
	}

	/**
	 * Create and upload batches using a CSV file. The file into the appropriate
	 * size batch files.
	 * 
	 * @param connection
	 *            Connection to use for creating batches
	 * @param jobInfo
	 *            Job associated with new batches
	 * @param csvFileName
	 *            The source file for batch data
	 */
	private List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection, JobInfo jobInfo, String csvFileName)
			throws IOException, AsyncApiException {
   		System.out.println( "createBatchesFromCSVFile start, file: " + csvFileName);
		List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(csvFileName)));
		// read the CSV header row
		byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
		int headerBytesLength = headerBytes.length;
		File tmpFile = File.createTempFile("bulkAPIUpdate", ".csv");

		// Split the CSV file into multiple batches
		try {
			FileOutputStream tmpOut = new FileOutputStream(tmpFile);
			int maxBytesPerBatch = 10000000; // 10 million bytes per batch
			int maxRowsPerBatch = 10000; // 10 thousand rows per batch
			int currentBytes = 0;
			int currentLines = 0;
			String nextLine;
			while ((nextLine = rdr.readLine()) != null) {
				byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
				// Create a new batch when our batch size limit is reached
				if (currentBytes + bytes.length > maxBytesPerBatch || currentLines > maxRowsPerBatch) {
					createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
					currentBytes = 0;
					currentLines = 0;
				}
				if (currentBytes == 0) {
					tmpOut = new FileOutputStream(tmpFile);
					tmpOut.write(headerBytes);
					currentBytes = headerBytesLength;
					currentLines = 1;
				}
				tmpOut.write(bytes);
				currentBytes += bytes.length;
				currentLines++;
			}
			// Finished processing all rows
			// Create a final batch for any remaining data
			if (currentLines > 1) {
				createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
			}
		} finally {
			tmpFile.delete();
		}
   		System.out.println( "createBatchesFromCSVFile end");
		return batchInfos;

	}

	/**
	 * Create a batch by uploading the contents of the file. This closes the
	 * output stream.
	 * 
	 * @param tmpOut
	 *            The output stream used to write the CSV data for a single
	 *            batch.
	 * @param tmpFile
	 *            The file associated with the above stream.
	 * @param batchInfos
	 *            The batch info for the newly created batch is added to this
	 *            list.
	 * @param connection
	 *            The BulkConnection used to create the new batch.
	 * @param jobInfo
	 *            The JobInfo associated with the new batch.
	 */
	private void createBatch(FileOutputStream tmpOut, File tmpFile, List<BatchInfo> batchInfos,
			BulkConnection connection, JobInfo jobInfo) throws IOException, AsyncApiException {
   		System.out.println( "createBatch start");
		tmpOut.flush();
		tmpOut.close();
		FileInputStream tmpInputStream = new FileInputStream(tmpFile);
		try {
			BatchInfo batchInfo = connection.createBatchFromStream(jobInfo, tmpInputStream);
			System.out.println(batchInfo);
			batchInfos.add(batchInfo);

		} finally {
			tmpInputStream.close();
		}
		System.out.println( "createBatch end");

	}

}