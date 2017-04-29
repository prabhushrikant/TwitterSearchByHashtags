package com.experticity.twitterAssignment;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Created by shrikant on 4/22/17.
 *
 * Class to provide utility functions like search twitter API based on hashtag(s) etc.
 * Also provides functionality to write to underlying datastore, elastic search in this case.
 */
public class IngestionUtil {

    private static final Logger LOGGER = Logger.getLogger(twitterLogger.class.getName());

    private static Twitter twitter = null;

    //get app properties or resources
    Properties defaultProps = AppInfo.getInstance().AppProps;

    public IngestionUtil()
    {
        //Configure and create twitter instance using keys specified in twitterApp.properties file
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(defaultProps.getProperty("twitterAPIKey"));
        cb.setOAuthConsumerSecret(defaultProps.getProperty("twitterAPISecretKey"));
        cb.setOAuthAccessToken(defaultProps.getProperty("twitterAccessToken"));
        cb.setOAuthAccessTokenSecret(defaultProps.getProperty("twitterAccessTokenSecret"));
        cb.setJSONStoreEnabled(true);

        TwitterFactory tf = new TwitterFactory(cb.build());
        twitter = tf.getInstance();
    }


    public List<Status> SearchTweetsByHashTags(HashSet<String> rcvdHashtags)
    {
        try
        {
            List<Status> AllTweets = null;

            StringBuilder queryString = new StringBuilder("");
            //searching for tweets containing all of these hashtags (AND condition)
            for (String hashtag : rcvdHashtags) {
                queryString.append(String.format("#%s+", hashtag));
            }
            //remove last + character.
            int len = queryString.length();
            String queryStringFinal = queryString.toString().substring(0,len-1);
            System.out.format("Searching for tweets containing hashtags %s\n", queryStringFinal);
            int maxTweetCount = 10000; //default;
            maxTweetCount = Integer.parseInt(defaultProps.getProperty("twitterMaxNumberOfTweetsToFetch"));
            Query query = new Query(queryStringFinal);
            query.setCount(100); //how many results per page.
            QueryResult result;

            do {

                result = twitter.search(query);
                List<Status> tweets = result.getTweets();

                if (AllTweets == null || AllTweets.isEmpty())
                    AllTweets = tweets;
                else
                    AllTweets.addAll(tweets);

            } while ((query = result.nextQuery()) != null && AllTweets.size() <= maxTweetCount);

            if (AllTweets != null && !AllTweets.isEmpty())
                System.out.println("Total "+ AllTweets.size() +" tweets collected");
            else
                System.out.println("Couldn't collect any tweets.");
            //DisplayTweets(AllTweets);

            IngestTweets(AllTweets);

            return AllTweets;
        }
        catch(TwitterException ext)
        {
            LOGGER.log(Level.SEVERE, "Something went wrong. Couldn't fetch tweets from twitter.", ext);
            throw new RuntimeException(ext); //rethrow the exception after logging.
        }

    }

    private void DisplayTweets(List<String> AllTweets)
    {
        int i = 1;

        System.out.println("Total "+ AllTweets.size() +" tweets received for hashtag.");

        for(String tweet : AllTweets)
        {
            System.out.format("Tweet#%d: %s\n", i, tweet);
            i++;
        }

    }


    private void IngestTweets(List<Status> AllTweets)
    {
        int i = 0;
        System.out.println("Ingesting tweets into datastore...");

        try (DataAccessUtil dataConnector = new DataAccessUtil())
        {
            dataConnector.createIndex();

            for (Status tweet : AllTweets) {

                //System.out.format("Tweet#%d: %s\n", i, tweet);
                //i++;

                tweet.getText();
                //Insert into datastore each tweet.
                boolean inserted = dataConnector.DSInsert(tweet, defaultProps.getProperty("elasticSearchIndexName"), defaultProps.getProperty("elasticSearchDocumentTypeName"));
                if(inserted)
                    i++;

            }
        }

        System.out.println("Finished ingesting "+ i +" tweets into datastore.");
    }





}
