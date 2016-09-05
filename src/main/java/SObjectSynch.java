import java.sql.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.*;
import java.util.*;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.lang.*;
import org.apache.commons.lang.time.DateUtils;

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

import org.json.JSONException;
import org.json.JSONObject;

import javax.xml.parsers.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.*;

import javax.xml.stream.events.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamConstants;

import com.sendgrid.*;

import redis.clients.jedis.Jedis;



public class SObjectSynch {
	private static final String USERNAME = System.getenv("SFDC_DEMO_USERNAME");
    private static final String PASSWORD = System.getenv("SFDC_DEMO_PASS");
    private static final String AUTH_ENDPOINT = System.getenv("PROD_AUTH_ENDPOINT");
    private static final String REST_ENDPOINT = System.getenv("PROD_REST_ENDPOINT");
    private static final String SG_TOKEN = System.getenv("SENDGRID_TOKEN");
    private static String lastSynchDT;
    private static String currentSynchDT;
    static PartnerConnection prtnrConnection;

    SObjectSynch(){

    }


	public Integer getUpdateCount (String objectName) {

		
		Integer count = null;

        try {

        	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			TimeZone utc = TimeZone.getTimeZone("UTC");
			dateFormat.setTimeZone(utc);

			//Connection for SOAP Query
			ConnectorConfig config = new ConnectorConfig();
	        config.setUsername(USERNAME);
	        config.setPassword(PASSWORD);
        	prtnrConnection = Connector.newConnection(config);
        	// display some current settings
            System.out.println("Auth EndPoint: "+config.getAuthEndpoint());
            System.out.println("Service EndPoint: "+config.getServiceEndpoint());
            System.out.println("Username: "+config.getUsername());
            System.out.println("SessionId: "+config.getSessionId());

            //Get the dt that we last synched this object in Redis.  
            Jedis jedis = getJedisConnection();
			lastSynchDT = jedis.get(objectName+"LastSynchDT");

            //Put now into our dt format;  We don't write to Redis until we're successful
            java.util.Date now = new java.util.Date();
			writeLogOutput("StandSynch", objectName + "LastSynchedDTFromRedis: " + lastSynchDT, false);
       		currentSynchDT = dateFormat.format(now)+"Z";

            QueryResult queryResult = prtnrConnection.query("SELECT COUNT() FROM " + objectName + " WHERE LastModifiedDate >= "+lastSynchDT + " AND LastModifiedDate <= "+currentSynchDT);
            count = queryResult.getSize();
            writeLogOutput("StandSynchCOUNT", "Result Size: "+count, true);

            //If we have 100K plus we need to run a larger dyno.  As yet, no way to do this programatically.
            //Send out alert and pause synch unless the override is set to true in Redis (Indicating we are on a larger dyno)
            if (count>100000) {
            	//May decide to have a scheduled job on the Apex side to monitor when we approach limits as well
            	//TODO: Create Redis key/value pair for LargeDynoOverride which we will set to true when the dyno
                //has been manually set to a large enough size for 100k + updates.  Set back to false when done.  this allows
                //for initial table loads and the occasional massive update
                if (Boolean.valueOf(jedis.get("LargeDynoOverride"))!=true) {
                    sendEmail("mprcic@stand.org", "Stand Connect has an upsert that exceeds 100,000 records on the " +objectName + " object.  The application will need to be restarted with a larger Dyno Size, and LargeDynoOverride set to True in Redis", "Bulk Data Load Exceeds 100k");
                    //Set Redis Table-synch for this object to Off so that we don't keep trying to update until we've upped the dyno size
                    jedis.set("Connect_Runnable", "false");
                    System.exit(1);
                }
            }

            jedis.close();

        } catch (ConnectionException e1) {
           e1.printStackTrace();
        } catch (URISyntaxException se) {
        	se.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }  

        return count;

	}


	public void soapFetch (String objectName, String fieldList) {
		writeLogOutput("StandSynchSOAP", "Starting SOAP Fetch", true);

		ConnectorConfig config = new ConnectorConfig();
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        try {
        	prtnrConnection = Connector.newConnection(config);
        	// display some current settings
            System.out.println("Auth EndPoint: "+config.getAuthEndpoint());
            System.out.println("Service EndPoint: "+config.getServiceEndpoint());
            System.out.println("Username: "+config.getUsername());
            System.out.println("SessionId: "+config.getSessionId());

            //TODO: Load & Update

            writeLogOutput("StandSynchSOAP", "Query Start", true);
            //Query for records
            QueryResult queryResults = prtnrConnection.queryAll("SELECT " + fieldList + " FROM " + objectName + " WHERE LastModifiedDate >= "+lastSynchDT + " AND LastModifiedDate <= " + currentSynchDT);
        
            writeLogOutput("StandSynchSOAP", "Query Finished", true);
            Integer count = queryResults.getSize();
            writeLogOutput("StandSynchCOUNT", "SOAP Q Result Size: "+count, true);
            //Convert queried field names into list for iterating below.  These should match the SF field names
            List <String> fieldNamesList = new ArrayList<String>(Arrays.asList(fieldList.split("\\s*,\\s*")));
            //Now create a string representing the field names, but formatted for Postgres
            fieldList = StringUtils.replaceOnce(fieldList,"Id","sfid");
            fieldList = fieldList.toLowerCase();
            String fieldNamesForQuery = fieldList;
            String fieldValuesForQuery = null;

            writeLogOutput("StandSynchSOAP", "Starting to loop through results.", true);
            if (queryResults.getSize() > 0) {
            	//Setup Connection for Postgres Query
            	Connection conn = getPGConnection();

            	//We need a map for this sobject of each field and its type so that we can appropriately cast the data on nulls
            	Map fieldMap = getFieldTypes(objectName, prtnrConnection);

            	//The query will return the first 500 records, we keep processing in batches of 500 until we are through all the results
            	boolean done = false;
            	while (!done) {
            		//build an upsert query for Postgres
            		String combinedQueriesForStatement = new String();
            		SObject[] records = queryResults.getRecords();
            		for (int i=0; i<records.length; i++) {
            			fieldValuesForQuery = null;
            			SObject sObj = (SObject)records[i];
            			for (String fieldName:fieldNamesList) {
                            String fieldType = fieldMap.get(fieldName).toString();
            				String fieldValueAsString;
            				if (sObj.getField(fieldName)==null) {
            					//Check the field type before proceeding
								if (fieldType.equals("double") || fieldType.equals("currency")) {
            						fieldValueAsString = "0";
            					} 
                                else {
            						fieldValueAsString = null;
            					}
            				} else {
								fieldValueAsString = StringEscapeUtils.escapeSql(sObj.getField(fieldName).toString());
            				}
            				if (fieldValuesForQuery == null) {
            					fieldValuesForQuery = "'"+fieldValueAsString+"'";
            				} else {
                                //if it is a null date field, add without quotes
                                if ((fieldType.equals("date") || fieldType.equals("timestamp") || fieldType.equals("datetime")) && fieldValueAsString==null) {
                                    fieldValuesForQuery = fieldValuesForQuery + "," + fieldValueAsString;
                                } else {
                                    fieldValuesForQuery = fieldValuesForQuery + ",'" + fieldValueAsString+"'";
                                }
            				}

            			}
            			//Upsert Query for this record
            			String upsertQuery = "INSERT INTO salesforce." + objectName + " ("+fieldNamesForQuery+") VALUES ("+fieldValuesForQuery+") ON CONFLICT (sfid) DO UPDATE SET ("+fieldNamesForQuery+") = ("+fieldValuesForQuery+")";
            			

            			if(combinedQueriesForStatement.length()==0) {
							combinedQueriesForStatement = upsertQuery;
						} else {
							combinedQueriesForStatement = combinedQueriesForStatement + "; " + upsertQuery;
						}
            		}
            		writeLogOutput("StandSynchSOAP", "PG Upsert Started", true);
            		//Do an upsert for the records in this batch
            		Statement stmt = conn.createStatement();
            		stmt.executeUpdate(combinedQueriesForStatement);
            		writeLogOutput("StandSynchSOAP", "PG Upsert Complete", true);

            		if(queryResults.isDone()) {
            			done = true;
            		} else {
            			queryResults = prtnrConnection.queryMore(queryResults.getQueryLocator());
            		}
            	}

            } else {
            	System.out.println("No Records Found");
            }
            Jedis jedis = getJedisConnection();
            jedis.set(objectName+"LastSynchDT", currentSynchDT);
            jedis.close();
            writeLogOutput("StandSynchSOAP", "All Upserts Done", true);

            //UPSERT into Postgres
            //Update Redis Last Synch DT
        } catch (Exception e) {
          e.printStackTrace();
        } 
	}

	private JobInfo createJob(String sobjectType, BulkConnection bulkConnection) {

			JobInfo job = new JobInfo();
			try {
				job.setObject(sobjectType);
	            job.setOperation(OperationEnum.query);
	            job.setConcurrencyMode(ConcurrencyMode.Parallel);
	            job.setContentType(ContentType.CSV);

	            job = bulkConnection.createJob(job);
	            System.out.println("JobID: "+job.getId());
			}  catch (AsyncApiException aae) {
	            aae.printStackTrace();
	        } 

			return job;
	}

	private void closeJob (BulkConnection connection, String jobId) throws AsyncApiException {
	    JobInfo job = new JobInfo();
	    job.setId(jobId);
	    job.setState(JobStateEnum.Closed);
	    connection.updateJob(job);
	}


	public void bulkFetchCSV (String objectName, String fieldList) {
		writeLogOutput("StandSynchBULK", "Starting Bulk Fetch", true);

        try {
        	//Get Bulk Connection
        	BulkConnection bulkConnection = getBulkConnection();
        	//Create Job
     		JobInfo job = createJob(objectName, bulkConnection);
     		//Set JobId
     		String jobId = job.getId();

     		//Query String
            String query = "SELECT " + fieldList + " FROM " + objectName + " WHERE LastModifiedDate >= "+lastSynchDT + " AND LastModifiedDate <= " + currentSynchDT; 
            
            //Create Batch
            //For Bulk queries, we create a single batch -- if the results are to big to be returned in one file, we will recieve multiple fiels, up to 15 files at 1Gig each https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_concepts_limits.htm
            ByteArrayInputStream inputStream = new ByteArrayInputStream(query.getBytes());
            writeLogOutput("StandSynchBULK", "Bulk Query Issued", true);
            BatchInfo batchInfo = bulkConnection.createBatchFromStream(job,inputStream);
            //Grab the batch ID
            String batchId = batchInfo.getId();

            //Close The Job: Closing the job ensures that processing of all batches (In this case one) can finish.
            closeJob(bulkConnection, jobId);

            //Setup Connection for Postgres Query
            Connection conn = getPGConnection();

            //Check for Status on Batch
            //TODO:  Right now, this will poll 10,000 times, waiting 2 seconds between each attempt, for a total of up to ~333 minutes
            //Use an alternative way to exit the query if it's been waiting to long without a postive or negative response on the batch result
			
            //Could do 1 sec initially, and then longer periods subsequently

			for(int i=0; i<10000; i++) {
            	System.out.println("Going to sleep For the " + i + "!!!!!!!!!!!!!");
				Thread.sleep(i==0 ? 2 * 1000 : 2 * 1000); //30 sec
				batchInfo = bulkConnection.getBatchInfo(jobId, batchId, ContentType.CSV);

				if (batchInfo.getState() == BatchStateEnum.Completed) {
					//Ok, when did we get info back
					writeLogOutput("StandSynchBULK", "Query Response Recieved from SF", true);
					//Now we need to fetch the actual results

					//get the query Result
					QueryResultList qrl = bulkConnection.getQueryResultList(jobId, batchId, ContentType.CSV);
                    Integer totalRecordsToProcess = batchInfo.getNumberRecordsProcessed();
                    writeLogOutput("StandSynchBULK", "TotalRecordsToProcess: "+totalRecordsToProcess, true);
					//Get the stream
					InputStream is = bulkConnection.getQueryResultStream(jobId, batchId, qrl.getResult()[0]);
					CSVReader rdr = new CSVReader(is);
					rdr.setMaxRowsInFile(1000000);
					rdr.setMaxCharsInFile(1000000000);

					//Variables to store query results and prep for Postgres Query
					List <String> resultHeader = rdr.nextRecord();
					String fieldNamesForQuery = StringUtils.join(resultHeader,", ");
					//In Postgres, the Id field from Salesforce is sfid
					fieldNamesForQuery = StringUtils.replaceOnce(fieldNamesForQuery,"Id","sfid");
					//All field names in Postgres are lowercase
					fieldNamesForQuery = fieldNamesForQuery.toLowerCase();
					int resultCols = resultHeader.size();
					List <String> row;

					String fieldValuesForQuery = new String();
					//We combine all the Postgres Queries into as few queries as possible
					//For right now I'm packing it all into one, but if there is an upper limit to the query string size I may need to split int multiples....TODO 
					String combinedQueriesForStatement = new String();

					//We need a map for this sobject of each field and its type so that we can appropriately cast the data on nulls
					Map fieldMap = getFieldTypes(objectName, prtnrConnection);
					//TODO:  Batch and do PG UPsert per batch
					//Set CSV Reader limits to real high
					writeLogOutput("StandSynchBULK", "Starting to Loop through CSV", true);
					Integer rowsInBatch = 0;
                    Integer totalRowsProcessed = 0;
					while((row = rdr.nextRecord())!=null) {
						rowsInBatch++;
                        totalRowsProcessed++;
						String fieldStringValue;
						Map <String, String> resultInfo = new HashMap<String, String>();
						for (int j=0;j<resultCols;j++) {
                            //Get the field type
                            String fieldType = fieldMap.get(resultHeader.get(j)).toString();
							if (row.get(j)==null) {
								if (fieldType.equals("double") || fieldType.equals("currency")) {
            						fieldStringValue = "0";
            					} else {
            						fieldStringValue = null;
            					}
							} else {
								fieldStringValue = row.get(j);
							}
							if (j==0) { //the first one is formatted differently
								fieldValuesForQuery="'"+StringEscapeUtils.escapeSql(fieldStringValue)+"'";
							} else {
                                //If it is a null date, add without quotes

                                if ((fieldType.equals("date") || fieldType.equals("timestamp") || fieldType.equals("datetime")) && fieldStringValue==null) {
                                    fieldValuesForQuery = fieldValuesForQuery + "," + fieldStringValue;
                                } else {
                                    fieldValuesForQuery = fieldValuesForQuery + ", '" + StringEscapeUtils.escapeSql(fieldStringValue) + "'";
                                }
								
							}
							//TODO:  better handling for null decimal fields
							resultInfo.put(resultHeader.get(j), row.get(j));
							
						}

						//Upsert to PG
						String upsertQuery = "INSERT INTO salesforce." + objectName + " ("+fieldNamesForQuery+") VALUES ("+fieldValuesForQuery+") ON CONFLICT (sfid) DO UPDATE SET ("+fieldNamesForQuery+") = ("+fieldValuesForQuery+")";
                        if(combinedQueriesForStatement.length()==0) {
							combinedQueriesForStatement = upsertQuery;
						} else {
							combinedQueriesForStatement = combinedQueriesForStatement + "; " + upsertQuery;
						}
						if (rowsInBatch == 2500 || totalRowsProcessed.equals(totalRecordsToProcess)) {
                            writeLogOutput("StandSynchBULK", "CreatingStatement--RowsProcessed: "+totalRowsProcessed, true);
                            writeLogOutput("StandSynchBULK", "CreatingStatement--totalRecordsToProcess: "+totalRecordsToProcess, true);
							writeLogOutput("StandSynchBULK", "PG Upsert Started, Combined Query Size in Bytes: " + combinedQueriesForStatement.getBytes().length, true);
							Statement stmt = conn.createStatement();
							stmt.executeUpdate(combinedQueriesForStatement);
							writeLogOutput("StandSynchBULK", "PG Upsert Completed", true);
							combinedQueriesForStatement = "";
							rowsInBatch=0;

						}						
						
					}

					break;

				} if (batchInfo.getState() == BatchStateEnum.Failed || batchInfo.getState() == BatchStateEnum.NotProcessed) {
					//TODO:  Handling for Failed Query
				} 
            }
        Jedis jedis = getJedisConnection();
        jedis.set(objectName+"LastSynchDT", currentSynchDT);
        jedis.close();
        writeLogOutput("StandSynchBULK", "All Upserts Done", true);
           
        } catch (Exception e) {
            e.printStackTrace();
        } 
	}

	public void writeLogOutput (String ht, String message, Boolean ts) {

		String logMessage = "#" + ht + " :Message: " + message;
		if (ts) {
			java.util.Date logDateTime= new java.util.Date();
			logMessage = logMessage + " :DT: " + new Timestamp(logDateTime.getTime());
		}

		System.out.println(logMessage);

	}


	private Connection getPGConnection() throws URISyntaxException, SQLException {
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

    private BulkConnection getBulkConnection() {

    	BulkConnection bulkConnection = null;
    	try {
			ConnectorConfig partnerConfig = new ConnectorConfig();
	        partnerConfig.setUsername(USERNAME);
	        partnerConfig.setPassword(PASSWORD);
	        partnerConfig.setAuthEndpoint(AUTH_ENDPOINT);
	        new PartnerConnection(partnerConfig);

			
			ConnectorConfig config = new ConnectorConfig();
			config.setSessionId(partnerConfig.getSessionId());
			System.out.println("SessionID BULK: "+partnerConfig.getSessionId());
	    	config.setUsername(USERNAME);
	        config.setPassword(PASSWORD);
	        config.setAuthEndpoint(AUTH_ENDPOINT);
	        config.setRestEndpoint(REST_ENDPOINT);
	        config.setCompression(true);
	        config.setTraceMessage(true);
	        //config.setPrettyPrintXml(true);

	    	bulkConnection=new BulkConnection(config);
	    	
    	} catch (ConnectionException e1) {
           e1.printStackTrace();
        } catch (AsyncApiException aae) {
            aae.printStackTrace();
        } 
        return bulkConnection;

    }

    private Map getFieldTypes (String sobjName, PartnerConnection conn) {
    	  Map < String, String > fieldMap = new HashMap < String, String>();
    	  try {
		    DescribeSObjectResult describeSObjectResult = conn.describeSObject(sobjName);
		    Field[] fields = describeSObjectResult.getFields();
		    // create a map of all fields with their type
		    fieldMap = new HashMap();
		    for (int i = 0; i < fields.length; i++) {
		      fieldMap.put(fields[i].getName().toString(), fields[i].getType().toString());
		    }

		  } catch (ConnectionException ce) {
		    ce.printStackTrace();
		  }

		  return fieldMap;


    }


    private static Jedis getJedisConnection() throws URISyntaxException {
        URI redisURI = new URI(System.getenv("REDIS_URL"));
        Jedis jedis = new Jedis(redisURI);
        return jedis;

    }

    public void sendEmail (String emailAddress, String message, String subject) {

    	try {
			SendGrid sendgrid = new SendGrid(SG_TOKEN);

		    SendGrid.Email email = new SendGrid.Email();

		    email.addTo(emailAddress);
		    email.setFrom("developer@stand.org");
		    email.setSubject(subject);
		    email.setHtml(message);

		    SendGrid.Response response = sendgrid.send(email);
		    System.out.println("#SendGrid Response: "+ response);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}

    	

    }











}