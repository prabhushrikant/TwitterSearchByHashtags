package com.experticity.twitterAssignment;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by shrikant on 4/22/17.
 * Program to read data from datastore - elastic search
 *
 */
public class ExtractionUtil {

    private static final Logger LOGGER = Logger.getLogger(twitterLogger.class.getName());

    //get app properties or resources
    Properties defaultProps = AppInfo.getInstance().AppProps;

    public ExtractionUtil()
    {
        //initialization code goes here.
    }

    //function to get datastore items containing hashtag
    //if parameter is empty it searches for all.
    //e.g. if indexName is empty it searches all the indices, or if hashtag is empty returns all the documents.
    public long getCount(String indexName, String type, String hashtag)
    {
        long result = 0;
        try(DataAccessUtil dataConnector = new DataAccessUtil()) {
            if ((indexName == null || indexName.isEmpty()) &&
                    (type == null || type.isEmpty()) &&
                    (hashtag == null || hashtag.isEmpty())) {
                result = dataConnector.getTotalCount();
            }
            else
            {
                result = dataConnector.getCount(indexName, type, hashtag);
            }
        }
        System.out.println("Total " + result + " data items found in datastore with hashtag : #"+hashtag);
        return result;
    }

    //function to return most commonly occuring hashtag with given hashtag
    public List<String> getRelevantHashtags(String indexName, String type, String hashtag)
    {
        if(hashtag == null || hashtag.isEmpty())
        {
            String ErrorMsg = "Can't find commonly occuring hashtag with an empty/null hashtag";
            LOGGER.log(Level.SEVERE, ErrorMsg);
            throw new UnsupportedOperationException(ErrorMsg);
        }

        //get all hashtags from document containing given hashtag put them in a map of hashtag vs count and return
        //ones with maximum count.
        try(DataAccessUtil dataConnector = new DataAccessUtil())
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.fetchSource("hashtagEntities.text", null);
            sourceBuilder.query(QueryBuilders.termsQuery("hashtagEntities.text", hashtag));

            SearchHits dataFound = dataConnector.getData(indexName, type, sourceBuilder);
            if(dataFound.totalHits() > 0)
            {
                //Hashmap to keep running count of each hashtag from search result.
                HashMap<String, Integer>finalMap = new HashMap<String, Integer>();

                for(SearchHit hit : dataFound.getHits())
                {
                    HashMap<String, Object> entitiesMap = (HashMap)hit.getSource();
                    for(Object entitiesValue : entitiesMap.values())
                    {
                        //HashMap<String, Object> hashtagsMap = (HashMap<String, Object>)entitiesValue;

                        //for(Object hashtagsValue : hashtagsMap.values())
                        //{
                            ArrayList hashtagsList = (ArrayList)entitiesValue;
                            for(Object textMap : hashtagsList)
                            {
                                //HashMap<String, Object> textMap1 = (HashMap<String,Object>)textMap;
                                for(Object textValue : ((HashMap<String,Object>)textMap).values())
                                {
                                    Integer count = 1;
                                    if(textValue.toString().toLowerCase().equals(hashtag.toLowerCase()) == false)
                                    {
                                        if (finalMap.containsKey(textValue.toString().toLowerCase())) {
                                            count = finalMap.get(textValue.toString().toLowerCase());
                                            count++;
                                        }
                                        finalMap.put(textValue.toString().toLowerCase(), count);
                                    }
                                }
                            }
                        //}
                    }
                }

                //sort Hashmap by values (counts) and return the top entry (Note: we shouldn't have an entry
                // in occurance hashmap with original hashtag we searched for).
                Set<Map.Entry<String, Integer>> entrySet = finalMap.entrySet();
                List<String> topHashtags = new ArrayList<String>();
                Integer maxOccurances = 0;
                for(Map.Entry<String, Integer> entry : entrySet)
                {
                    if(entry.getValue() > maxOccurances)
                    {
                        topHashtags.clear();
                        topHashtags.add(entry.getKey());
                        maxOccurances = entry.getValue();
                    }
                    else if(entry.getValue() == maxOccurances)
                    {
                        topHashtags.add(entry.getKey());
                    }
                }
                //return hashtag values in topEntries
                System.out.println("Most commonly occurring hashtags with #"+ hashtag + " are:");
                for(String topHashtag : topHashtags)
                {
                    System.out.println("#"+topHashtag);
                }
                return topHashtags;
            }
        }

        return null;
    }

}


