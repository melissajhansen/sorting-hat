import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
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
			
			//Build map to store the point value of each Answer
			HashMap< String, String> contactSchoolMap = new HashMap< String, String>();

			Statement answerStmt = conn.createStatement(); 
			ResultSet answerValues = answerStmt.executeQuery("SELECT answer_values.question_id, answer_values.answer_value, answer_values.gry_value, answer_values.rav_value, answer_values.huff_value, answer_values.sly_value, questions.question_field_name FROM sort.answer_values INNER JOIN sort.questions ON answer_values.question_id=questions.question_id");
			

			//Build out a map of the answers indexed by the question field name
			HashMap < String, Answer > answerMap = new HashMap < String, Answer >();
			while (answerValues.next()) {
				Answer ans = new Answer(answerValues.getString("question_id"), answerValues.getString("answer_value"), answerValues.getInt("gry_value"), answerValues.getInt("rav_value"), answerValues.getInt("huff_value"), answerValues.getInt("sly_value"));
				//the key is the quesion field name concatenated with the answer value
				answerMap.put(answerValues.getString("question_field_name")+ans.answer_value, ans);
			}

			//Key format: Heads_or_Tails__cHeads
	
			//Query all Contacts who need a school assigment
			Statement conStmt = conn.createStatement(); 
			ResultSet studentsToSort = conStmt.executeQuery("SELECT salesforce.contact.sfid, salesforce.contact.dawn_or_dusk__c, salesforce.contact.heads_or_tails__c, salesforce.contact.dont_call_me__c, salesforce.contact.what_instrument__c, salesforce.contact.which_smell__c  FROM salesforce.contact WHERE (salesforce.contact.CreatedDate > current_date - interval '1 week' OR salesforce.contact.LastModifiedDate >= current_date - interval '1 week' )");
			
			//This pattern is for demo purposes only.  For Production we would want the logic independent of the particular fields
			List < String > questionFieldNames = Arrays.asList("Dawn_or_Dusk__c","Heads_or_Tails__c","Dont_Call_Me__c","What_Instrument__c","Which_Smell__c");

			//Loop through the students, updating the score for each school based on answers
			while(studentsToSort.next()) {
				String contactId = studentsToSort.getString("sfid");
				HashMap < String, Integer > scoreMap = new HashMap < String, Integer >();
				scoreMap.put("Gryffindor", 0);
				scoreMap.put("Ravenclaw", 0);
				scoreMap.put("Hufflepuff", 0);
				scoreMap.put("Slytherin", 0);
				for (String q: questionFieldNames) {
					System.out.println("#SortingHat-q: "+q);
					System.out.println("#SortingHat-getquestion: "+studentsToSort.getString(q));
					Answer a = answerMap.get(q+studentsToSort.getString(q));
					System.out.println("#SortingHat-answer: "+a);
					scoreMap.put("Gryffindor",scoreMap.get("Gryffindor")+a.gry_value);
					scoreMap.put("Ravenclaw",scoreMap.get("Ravenclaw")+a.rav_value);
					scoreMap.put("Hufflepuff",scoreMap.get("Hufflepuff")+a.huff_value);
					scoreMap.put("Slytherin",scoreMap.get("Slytherin")+a.sly_value);

					System.out.println("#SortingHat-scoremap: "+scoreMap);
				}

				//Great, now we have all four score totals.  Assign them to the correct house based on the highest score
				String selectedSchool = getMaxEntry(scoreMap);
				contactSchoolMap.put(contactId, selectedSchool);


			}

			//SchoolFinder.updateSchoolsCount(accountSchoolCountMap);

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


    //returns the key associated with the largest value.  If there are multiple keys with the same value, returns the last one
    public static String getMaxEntry(Map<String, Integer> map) {        
	    Entry<String, Integer> maxEntry = null;
	    Integer max = Collections.max(map.values());

	    for(Entry<String, Integer> entry : map.entrySet()) {
	        Integer value = entry.getValue();

	        if(null != value && max == value) {
	            maxEntry = entry;
	        }
	    }

	    return maxEntry.getKey();
	}

}
