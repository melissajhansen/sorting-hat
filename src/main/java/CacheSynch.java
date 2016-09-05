import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.client.ClientSessionChannel.MessageListener;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;

import org.apache.commons.lang.time.DateUtils;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.*;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;

import redis.clients.jedis.Jedis;

////

/**
 * Listen for updates to our synched tables.  Update the cache (currently Postgres)
 **/

public class CacheSynch {

    // This URL is used only for logging in. The LoginResult 
    // returns a serverUrl which is then used for constructing
    // the streaming URL. The serverUrl points to the endpoint 
    // where your organization is hosted.

    static final String LOGIN_ENDPOINT = System.getenv("PROD_LOGIN_ENDPOINT");
    private static final String USERNAME = System.getenv("SFDC_DEMO_USERNAME");
    private static final String PASSWORD = System.getenv("SFDC_DEMO_PASS");

    static PartnerConnection connection;

    private static final boolean USE_COOKIES = false;

    // The channel to subscribe to. Same as the name of the PushTopic. 
    // Be sure to create this topic before running this sample.
    private static final String CONTACT_CHANNEL = "/topic/ContactUpdates";
    private static final String STREAMING_ENDPOINT_URI = "/cometd/36.0";

    // The long poll duration.
    private static final int CONNECTION_TIMEOUT = 20 * 1000;  // milliseconds
    private static final int READ_TIMEOUT = 120 * 1000; // milliseconds

    public static void main(String[] args) throws Exception {

    	System.out.println("Running streaming client....");

        //Check to see whether synch is runnable
        Jedis jedis = getJedisConnection();
        if (Boolean.valueOf(jedis.get("Connect_Runnable"))!=true) {
            return;
        }
        jedis.close();

        final BayeuxClient client = makeClient();
        client.getChannel(Channel.META_HANDSHAKE).addListener
            (new ClientSessionChannel.MessageListener() {

            public void onMessage(ClientSessionChannel channel, Message message) {

            	System.out.println("[CHANNEL:META_HANDSHAKE]: " + message);

                boolean success = message.isSuccessful();
                if (!success) {
                    String error = (String) message.get("error");
                    if (error != null) {
                        System.out.println("Error during HANDSHAKE: " + error);
                        System.out.println("Exiting...");
                        System.exit(1);
                    }

                    Exception exception = (Exception) message.get("exception");
                    if (exception != null) {
                        System.out.println("Exception during HANDSHAKE: ");
                        exception.printStackTrace();
                        System.out.println("Exiting...");
                        System.exit(1);

                    }
                }
            }

        });

        client.getChannel(Channel.META_CONNECT).addListener(
            new ClientSessionChannel.MessageListener() {
            public void onMessage(ClientSessionChannel channel, Message message) {

                System.out.println("[CHANNEL:META_CONNECT]: " + message);

                boolean success = message.isSuccessful();
                if (!success) {
                    String error = (String) message.get("error");
                    if (error != null) {
                        System.out.println("Error during CONNECT: " + error);
                        System.out.println("Exiting...");
                        System.exit(1);
                    }
                } 
            }

        });

        client.getChannel(Channel.META_SUBSCRIBE).addListener(
            new ClientSessionChannel.MessageListener() {

            public void onMessage(ClientSessionChannel channel, Message message) {

            	System.out.println("[CHANNEL:META_SUBSCRIBE]: " + message);
                boolean success = message.isSuccessful();
                if (!success) {
                    String error = (String) message.get("error");
                    if (error != null) {
                        System.out.println("Error during SUBSCRIBE: " + error);
                        System.out.println("Exiting...");
                        System.exit(1);
                    }
                }
            }
        });



        client.handshake();
        System.out.println("Waiting for handshake");
        
        boolean handshaken = client.waitFor(10 * 1000, BayeuxClient.State.CONNECTED);
        if (!handshaken) {
            System.out.println("Failed to handshake: " + client);
            System.exit(1);
        }



        System.out.println("Subscribing for channel: " + CONTACT_CHANNEL);
        //TODO: Reconnect after long jobs....
        client.getChannel(CONTACT_CHANNEL).subscribe(new MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel channel, Message message) {

                System.out.println("Received Message: " + message);
                //Set some sample start/end dates for testing
                java.util.Date startDate= new java.util.Date();
                System.out.println("#StandSynch Timestamp message Retrieved"+new Timestamp(startDate.getTime()));


                //Some Log info
                JSONObject obj = new JSONObject(message);
                String accountId = obj.getJSONObject("data").getJSONObject("sobject").getString("Id");
                //Get the Operation Type so we know what to do
                String operationType = obj.getJSONObject("data").getJSONObject("event").getString("type");


                //********************************Start Working with Streaming Message**********************

                //1.  Determine how many records have been updated since the last synch

                //Initialize SObjectSynch
                SObjectSynch synch = new SObjectSynch();
                Integer updateSize = synch.getUpdateCount("Contact");

                //Fields to query.
                String queryFields = "Id, Dawn_or_Dusk__c, Heads_or_Tails__c, Dont_Call_Me__c, What_Instrument__c, Which_Smell__c, LastModifiedDate, CreatedDate";

                //2. Call the SOAPFetch or BulkFetch depending on the count
                if (updateSize != null && updateSize >=1) {
                    if (updateSize <= 20000) {
                        synch.soapFetch("Contact", queryFields);
                    } else {
                        //use the bulk loader since we are over 20000
                        synch.bulkFetchCSV("Contact", queryFields);
                    }
                }
            }
        });



        System.out.println("Waiting for streamed account data from your organization ...");
        // while (true) {
        //     // This infinite loop is for demo only,
        //     // to receive streamed events on the 
        //     // specified topic from your organization.
        // }
    }


    private static BayeuxClient makeClient() throws Exception {
        HttpClient httpClient = new HttpClient();
        httpClient.setConnectTimeout(CONNECTION_TIMEOUT);
        httpClient.setTimeout(READ_TIMEOUT);
        httpClient.start();

        String[] pair = SoapLoginUtil.login(httpClient, USERNAME, PASSWORD);
        
        if (pair == null) {
            System.exit(1);
        }

        assert pair.length == 2;
        final String sessionid = pair[0];
        String endpoint = pair[1];
        System.out.println("Login successful!\nServer URL: " + endpoint 
            + "\nSession ID=" + sessionid);

        Map<String, Object> options = new HashMap<String, Object>();
        options.put(ClientTransport.TIMEOUT_OPTION, READ_TIMEOUT);
        LongPollingTransport transport = new LongPollingTransport(
          options, httpClient) {

        	@Override
            protected void customize(ContentExchange exchange) {
                super.customize(exchange);
                exchange.addRequestHeader("Authorization", "OAuth " + sessionid);
            }
        };

        BayeuxClient client = new BayeuxClient(salesforceStreamingEndpoint(
            endpoint), transport);
        if (USE_COOKIES) establishCookies(client, USERNAME, sessionid);
        return client;
    }

    private static String salesforceStreamingEndpoint(String endpoint)
        throws MalformedURLException {
        return new URL(endpoint + STREAMING_ENDPOINT_URI).toExternalForm();
    }

    private static void establishCookies(BayeuxClient client, String user, 
        String sid) {
        client.setCookie("com.salesforce.LocaleInfo", "us", 24 * 60 * 60 * 1000);
        client.setCookie("login", user, 24 * 60 * 60 * 1000);
        client.setCookie("sid", sid, 24 * 60 * 60 * 1000);
        client.setCookie("language", "en_US", 24 * 60 * 60 * 1000);
    }

    private static Jedis getJedisConnection() throws URISyntaxException {
        URI redisURI = new URI(System.getenv("REDIS_URL"));
        Jedis jedis = new Jedis(redisURI);
        return jedis;
    }

}
