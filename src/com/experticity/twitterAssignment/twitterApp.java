package com.experticity.twitterAssignment;

import javax.naming.OperationNotSupportedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class twitterApp {

    enum OperationType { None, Ingestion, Extraction}

    private static final Logger LOGGER = Logger.getLogger(twitterLogger.class.getName());

    private static HashSet<String> _hashTagsToSearch = null;
    private static OperationType _reqOperation = OperationType.None;

    public static void main(String[] args) throws Exception {

        try {

            if(args.length > 0)
            {
                //get all the specified arguments
                for (String currArg : args)
                {
                    String[] strArr = currArg.split("=");
                    if(strArr.length >= 2)
                    {
                        switch(strArr[0].toLowerCase().trim())
                        {
                            case "op":
                            case "operation":{

                                if(strArr[1].trim().equalsIgnoreCase("ingest") ||
                                        strArr[1].trim().equalsIgnoreCase("ingestion"))
                                {
                                    _reqOperation = OperationType.Ingestion;
                                }
                                else if(strArr[1].trim().equalsIgnoreCase("extract") ||
                                        strArr[1].trim().equalsIgnoreCase("extraction"))
                                {
                                    //extract specified hashtags from datastore
                                    _reqOperation = OperationType.Extraction;
                                }
                                else
                                {
                                    throw new OperationNotSupportedException("Invalid operation specified. Can only take" +
                                            "values like \"ingest\" or \"extract\"");
                                }

                                break;
                            }
                            case "h":
                            case "hashtags": {
                                _hashTagsToSearch = getListOfHashtags(strArr[1]);
                                break;
                            }
                            //TODO:
                            case "f":
                            case "file":{

                                break;
                            }
                            default: {
                                throw new UnsupportedOperationException("Invalid argument option specified. No such option");
                            }

                        }
                    }
                    else
                    {
                        throw new UnsupportedOperationException("Argument not specified in correct format. Please specify" +
                                "parameter as hashtags=<value> or h=<value>");

                    }

                }

            }
            else
            {
                throw new UnsupportedOperationException("Invalid number of parameters passed. Please specify hashtag(s)" +
                        "separated by colons e.g. hashtags=fitness,sports or specify file containing hashtag values.");

            }

            //start the application.
            start();

        }
        catch (Exception ex)
        {
            LOGGER.log(Level.SEVERE, "Something went wrong. Can't continue the application.", ex);
            throw ex; //rethrow the exception after logging.
        }


    } //end of main


    private static HashSet<String> getListOfHashtags(String rcvdCommaSeparatedHashtags)
    {
        HashSet<String> result = new HashSet<String>();
        //reads the list of hashtags values provided from comma separated.
        for (String hashtag : rcvdCommaSeparatedHashtags.split(","))
        {
            String validHashtag = null;
            if(hashtag.isEmpty() == false){

                //pre-process hashtag for having # in value
                if (hashtag.startsWith("#"))
                {
                    //strip of leading # symbols
                    int index = hashtag.lastIndexOf("#");
                    validHashtag = hashtag.substring(index+1);
                }
                else validHashtag = hashtag;

                if (!validHashtag.isEmpty() && !result.contains(validHashtag))
                    result.add(validHashtag);
            }

        }

        return result;
    }

    //application start.
    private static void start()
    {
        if(_reqOperation == OperationType.Ingestion)
        {
            IngestionUtil ingester = new IngestionUtil();
            ingester.SearchTweetsByHashTags(_hashTagsToSearch);
        }
        else if(_reqOperation == OperationType.Extraction)
        {
            Properties defaultProp = AppInfo.getInstance().AppProps;
            String indexName = defaultProp.getProperty("elasticSearchIndexName");
            String type = defaultProp.getProperty("elasticSearchDocumentTypeName");
            ExtractionUtil extractor = new ExtractionUtil();
            extractor.getCount(indexName,type,_hashTagsToSearch.toArray()[0].toString());
            extractor.getRelevantHashtags(indexName,type,_hashTagsToSearch.toArray()[0].toString());
        }
    }
}
