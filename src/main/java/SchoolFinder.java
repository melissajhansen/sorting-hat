import java.sql.*;
import java.util.*;
import java.io.*;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.*;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.*;

import com.heroku.sdk.jdbc.DatabaseUrl;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.async.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;

import java.lang.StringBuilder;
import java.lang.Object;
import org.apache.commons.collections4.ListUtils;



public class SchoolFinder {

	private static final String LOGIN_ENDPOINT = System.getenv("PROD_LOGIN_ENDPOINT");
	private static final String AUTH_ENDPOINT = System.getenv("PROD_AUTH_ENDPOINT");
	private static final String USERNAME = System.getenv("SFDC_DEMO_USERNAME");
    private static final String PASSWORD = System.getenv("SFDC_DEMO_PASS");

    static PartnerConnection connection;

	//Currently runs nightly via a Heroku sheduled Job.
	//Future: will also rescore as SObject Synch runs on Contacts

	public static void main(String[] args) throws IOException, Exception {

		try {
			System.out.println("Starting to run....");
			//Setup Connection for Postgres Query
	        Connection conn = getPGConnection();
			Statement stmt = conn.createStatement();
			
			//Build map to store School Count
			HashMap< String, Integer> accountSchoolCountMap = new HashMap< String, Integer>();

			//query for and build a map of each zip code and the count of schools in each
			ResultSet schools = stmt.executeQuery("SELECT schools.schooldata.lzip09 FROM schools.schooldata");
			HashMap< String, Integer> zipCodeMap = new HashMap< String, Integer>();
			while(schools.next()) {
				String zip = schools.getString("lzip09");
				//if the zip code is in the map, increment the count
				if (zipCodeMap.get(zip)!=null) {
					Integer mapCount = zipCodeMap.get(zip);
					int count = ++mapCount;
					zipCodeMap.put(zip, count);
				} else {
				//otherwise add to the map with a count of one
					zipCodeMap.put(zip,1);
				}
			}
			

			//Criteria: all Accounts added or Updated in the last week
			Statement acctStmt = conn.createStatement(); 
			ResultSet accountsToUpdate = acctStmt.executeQuery("SELECT salesforce.account.sfid, salesforce.account.name, salesforce.account.BillingPostalCode FROM salesforce.account WHERE (salesforce.account.CreatedDate > current_date - interval '1 week' OR salesforce.account.LastModifiedDate >= current_date - interval '1 week' )");
			
			//Loop through the accounts, updating the count of schools nearby
			while(accountsToUpdate.next()) {
				String accountId = accountsToUpdate.getString("sfid");
				String accountZip = accountsToUpdate.getString("BillingPostalCode");
				int totalCount = 0;
				//if there is no, leave it at 0
				if (accountZip!=null && accountZip!="") {
					//look for the zip code in our map.  IF it's there, grab the count for our Account
					if (zipCodeMap.get(accountZip)!=null) {
						totalCount = zipCodeMap.get(accountZip);
					}
				}

				//Add the count to our map
				accountSchoolCountMap.put(accountId, totalCount);
			}

			SchoolFinder.updateSchoolsCount(accountSchoolCountMap);

    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}

	}

	public static void updateSchoolsCount (HashMap< String, Integer> schoolCountMap) throws AsyncApiException, ConnectionException, IOException {

		BulkConnection bulkConn = getBulkConnection();

		//file writer
		//We will put all updates in a csv for bulk processing
		FileWriter fileWriter = null;
		try {

			fileWriter = new FileWriter("SchoolCountUpdates");
			//put in the header row
			fileWriter.append("ID,Schools_in_Zipcode_Count__c\n");
			
			int listSize=schoolCountMap.size();
			// SObject[] records = new SObject[listSize];

	        Iterator it = schoolCountMap.entrySet().iterator();

	        for (int i=0; i<listSize; i++) {
	        	Map.Entry account = (Map.Entry) it.next();
	        	fileWriter.append(String.valueOf(account.getKey()));
	        	fileWriter.append(",");
	        	fileWriter.append(String.valueOf(account.getValue()));
	        	fileWriter.append("\n");
	        }

		} catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
			}
			
		}

		//Ok, now let's do a bulk create 
		JobInfo job = createJob("Account", bulkConn);
        List<BatchInfo> batchInfoList = createBatchesFromCSVFile(bulkConn, job, "SchoolCountUpdates");
        closeJob(bulkConn, job.getId());
        awaitCompletion(bulkConn, job, batchInfoList);
        checkResults(bulkConn, job, batchInfoList);
	
	}

	// /**
	//  * Create a new job using the Bulk API.
	//  * 
	//  * @param sobjectType
	//  *            The object type being loaded, such as "Account"
	//  * @param connection
	//  *            BulkConnection used to create the new job.
	//  * @return The JobInfo for the new job.
	//  * @throws AsyncApiException
	//  */
	private static JobInfo createJob(String sobjectType, BulkConnection connection) throws AsyncApiException {
	    JobInfo job = new JobInfo();
	    job.setObject(sobjectType);
	    job.setOperation(OperationEnum.update);
	    job.setContentType(ContentType.CSV);
	    job = connection.createJob(job);
	    System.out.println(job);
	    return job;
	}

	private static void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }

    private static void awaitCompletion(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList) throws AsyncApiException {
        long sleepTime = 0L;
        Set<String> incomplete = new HashSet<String>();
        for (BatchInfo bi : batchInfoList) {
            incomplete.add(bi.getId());
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
            System.out.println("Awaiting results..." + incomplete.size());
            sleepTime = 10000L;
            BatchInfo[] statusList =
              connection.getBatchInfoList(job.getId()).getBatchInfo();
            for (BatchInfo b : statusList) {
                if (b.getState() == BatchStateEnum.Completed
                  || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
                        System.out.println("BATCH STATUS:\n" + b);
                    }
                }
            }
        }
    }

    private static void checkResults(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList) throws AsyncApiException, IOException {
        // batchInfoList was populated when batches were created and submitted
        for (BatchInfo b : batchInfoList) {
            CSVReader rdr =
              new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new HashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }
                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                boolean created = Boolean.valueOf(resultInfo.get("Created"));
                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");
                if (success && created) {
                    System.out.println("Created row with id " + id);
                } else if (!success) {
                    System.out.println("Failed with error: " + error);
                }
            }
        }
    }


    private static List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection, JobInfo jobInfo, String csvFileName) throws IOException, AsyncApiException {
        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
        BufferedReader rdr = new BufferedReader(
            new InputStreamReader(new FileInputStream(csvFileName))
        );
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
                if (currentBytes + bytes.length > maxBytesPerBatch
                  || currentLines > maxRowsPerBatch) {
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
        return batchInfos;
    }

    private static void createBatch(FileOutputStream tmpOut, File tmpFile, List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo) throws IOException, AsyncApiException {
        tmpOut.flush();
        tmpOut.close();
        FileInputStream tmpInputStream = new FileInputStream(tmpFile);
        try {
            BatchInfo batchInfo =
              connection.createBatchFromStream(jobInfo, tmpInputStream);
            System.out.println(batchInfo);
            batchInfos.add(batchInfo);

        } finally {
            tmpInputStream.close();
        }
    }


		/**
	 * Create the BulkConnection used to call Bulk API operations.
	 */
	private static BulkConnection getBulkConnection() throws ConnectionException, AsyncApiException {
	    ConnectorConfig partnerConfig = new ConnectorConfig();
	    partnerConfig.setUsername(USERNAME);
	    partnerConfig.setPassword(PASSWORD);
	    partnerConfig.setAuthEndpoint(AUTH_ENDPOINT);

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
	    String apiVersion = "37.0";
	    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
	    config.setRestEndpoint(restEndpoint);
	    // This should only be false when doing debugging.
	    config.setCompression(true);
	    // Set this to true to see HTTP requests and responses on stdout
	    config.setTraceMessage(false);
	    BulkConnection connection = new BulkConnection(config);
	    return connection;
	}


	private static Connection getPGConnection() throws URISyntaxException, SQLException {
        URI dbUri = new URI(System.getenv("DATABASE_URL"));

        String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + dbUri.getPath();
       
		//when using a local db there is no username/password       
        if (dbUri.getUserInfo().length() == 0) {
        	return DriverManager.getConnection(dbUrl);
        } else {
        	String username = dbUri.getUserInfo().split(":")[0];
        	String password = dbUri.getUserInfo().split(":")[1];
        	return DriverManager.getConnection(dbUrl, username, password);
        }
    }
}
