package guru.baithak.example.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterApp {
   static Logger logger=Logger.getLogger(TwitterApp.class);

    // Below key and secrets can be regenerated and hence I am pushing it on github.
    static String CONSUMER_KEY="kivnqUBADEvT5raBxD0PuyOeQ";
    static String CONSUMER_SECRETS="aTG8dkU0lK1WFVJXXirmtJb50iq5EIXzddpEnneew05oYg4o6g";
    static String TOKEN="1495370716769763338-TH5WibFtjIzHOQHuNQGAcaQ74072RO";
    static String SECRET="M2BCKZ00fcNCUYfiq8jej0yNmXuDvu6s00EfNyUaPALlW";

    public static void main(String[] args) {
        new TwitterApp().run();
    }

    public  void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client =  createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.info("Something went wrong !!");
            }
            logger.info("Received tweet: " + msg);
        }
    }

    public static Client createTwitterClient(BlockingQueue msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("train");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRETS, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }
}
