package com.experticity.twitterAssignment;

import com.google.gson.Gson;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.json.DataObjectFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;



/**
 * Created by shrikant on 4/26/17.
 * Program to access datastore (such as elasticsearch in this case)
 */
public class DataAccessUtil implements AutoCloseable{

    private static final Logger LOGGER = Logger.getLogger(twitterLogger.class.getName());
    private TransportClient elasticSearchClient = null;
    private Properties defaultProps = AppInfo.getInstance().AppProps;

    //getter
    public TransportClient getElasticSearchClient()
    {
        return this.elasticSearchClient;
    }

    public DataAccessUtil()
    {
        if(!TestDataStoreConnection()){
            String ErrorMsg = "Couldn't connect to datastore.";
            LOGGER.log(Level.SEVERE, ErrorMsg);
            throw new RuntimeException(ErrorMsg);
        }

    }

    //Impementing interface method.
    public void close()
    {
        //closing the connection to datastore.
        elasticSearchClient.close();
    }

    //Function to check if datastore is up and running.
    private boolean TestDataStoreConnection()
    {
        try {

            String serverName = defaultProps.getProperty("elasticSearchServerName");
            int ServerPort_Int = Integer.parseInt(defaultProps.getProperty("elasticSearchPortNo"));

            TransportAddress serverAddress = new InetSocketTransportAddress(InetAddress.getByName(serverName),ServerPort_Int);
            elasticSearchClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(serverAddress);
            ClusterHealthStatus healthStatus = elasticSearchClient.admin().cluster().prepareClusterStats().execute().actionGet().getStatus();

            if (healthStatus != null)
                return true;
            else
                return false;

        }
        catch(UnknownHostException exn)
        {
            String ErrorMsg = "Invalid host name specified for datastore";
            LOGGER.log(Level.SEVERE, ErrorMsg, exn);
            throw new RuntimeException(ErrorMsg, exn);
        }
    }

    //function create index using app properties.
    public void createIndex(){
        defaultProps = AppInfo.getInstance().AppProps;

        String indexName = "";
        if (defaultProps.getProperty("elasticSearchIndexName") == null ||
                defaultProps.getProperty("elasticSearchIndexName").isEmpty())
            throw new UnsupportedOperationException("Index name to store the data is not specified.");
        else
            indexName = defaultProps.getProperty("elasticSearchIndexName");

        int number_of_shards = 3; //default
        if (defaultProps.getProperty("elasticSearchNumberOfShards") != null &&
                defaultProps.getProperty("elasticSearchNumberOfShards").isEmpty() == false)
            number_of_shards = Integer.parseInt(defaultProps.getProperty("elasticSearchNumberOfShards"));

        int number_of_replicas = 2; //default
        if (defaultProps.getProperty("elasticSearchNumberOfReplicas") != null &&
                defaultProps.getProperty("elasticSearchNumberOfReplicas").isEmpty() == false)
            number_of_replicas = Integer.parseInt(defaultProps.getProperty("elasticSearchNumberOfReplicas"));

        int number_of_total_mappingFields = 1500; //default
        if (defaultProps.getProperty("elasticSearchTotalMappingFields") != null &&
                defaultProps.getProperty("elasticSearchTotalMappingFields").isEmpty() == false)
            number_of_total_mappingFields = Integer.parseInt(defaultProps.getProperty("elasticSearchNumberOfReplicas"));

        createIndex(indexName, number_of_shards, number_of_replicas, number_of_total_mappingFields);
    }

    //Function to create elasticsearch index if doesn't exists.
    public void createIndex(String indexName, int number_of_shards, int number_of_replicas, int number_of_total_mappingFields)
    {
        try {
            IndicesAdminClient adminClient = elasticSearchClient.admin().indices();
            if (!adminClient.prepareExists(indexName).execute().actionGet().isExists()) {
                LOGGER.log(Level.INFO, "Creating new elastic search index " + indexName);


                XContentBuilder builder = jsonBuilder()
                        .startObject()
                        .startObject("settings")
                        .startObject("index")
                                .field("number_of_shards",number_of_shards)
                                .field("number_of_replicas",number_of_replicas)
                                .field("mapping.total_fields.limit",number_of_total_mappingFields)
                        .endObject()
                        .endObject()
                        .endObject();
                adminClient.prepareCreate(indexName).setSource(builder).execute().actionGet();
            } else {
                LOGGER.log(Level.INFO, "Index \"" + indexName + "\" already exists.");
            }
        }
        catch(IOException ex)
        {
            LOGGER.log(Level.SEVERE, "IO exception while creating elasticsearch index", ex);
            throw new RuntimeException(ex);
        }
    }

    //Function to insert data into datastore under specific index
    public boolean DSInsert(Status TweetToInsert, String IndexName, String DocumentType)
    {
        //String json = TwitterObjectFactory.getRawJSON(TweetToInsert); //found that this is faulty...and gives nulls
        Gson gson = new Gson();
        String json = gson.toJson(TweetToInsert);

        if(json != null && json.isEmpty() == false) {
            elasticSearchClient.prepareIndex(IndexName, DocumentType).setSource(json, XContentType.JSON).get();
            return true;
        }
        else
            return false;
    }

    //function to get datastore items containing all of these hashtags (AND conditions)
    //if parameter is empty it searches for all.
    //e.g. if indexName is empty it searches all the indices, or if hashtags are empty returns all the documents.
    public long getTotalCount()
    {
        SearchResponse response = elasticSearchClient.prepareSearch().get();
        if(response != null)
            return response.getHits().totalHits();
        else
            return 0;
    }

    public long getCount(String indexName, String type, String hashtag)
    {
        SearchResponse response = elasticSearchClient.prepareSearch(indexName)
                .setTypes(type)
                //.setQuery(QueryBuilders.termsQuery("entities.hashtags.text", hashtag)).get();
                .setQuery(QueryBuilders.termsQuery("hashtagEntities.text", hashtag)).get();

        if(response != null)
            return response.getHits().totalHits();
        else
            return 0;
    }

    //Function to get required data items matching query criteria.
    public SearchHits getData(String indexName, String type, SearchSourceBuilder sourceFilter)
    {
        SearchResponse response = elasticSearchClient.prepareSearch(indexName)
                .setTypes(type)
                .setSource(sourceFilter)
                .setSize(1000)
                .get();

        if(response != null)
            return response.getHits();
        else
            return null;
    }
}
