package com.experticity.twitterAssignment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by shrikant on 4/26/17.
 * Singleton class to get app properties.
 */
public class AppInfo {

    private static final Logger LOGGER = Logger.getLogger(twitterLogger.class.getName());

    private static AppInfo ourInstance = new AppInfo();

    public Properties AppProps = null;

    public static AppInfo getInstance() {
        return ourInstance;
    }

    private AppInfo() {
        try {
            //load properties or resources
            AppProps = new Properties();
            FileInputStream in = new FileInputStream("./twitterApp.properties");
            AppProps.load(in);
            in.close();

            if(AppProps.getProperty("twitterAPIKey") == null ||
                    AppProps.getProperty("twitterAPIKey").isEmpty() ||
                    AppProps.getProperty("twitterAPISecretKey") == null ||
                    AppProps.getProperty("twitterAPISecretKey").isEmpty() ||
                    AppProps.getProperty("twitterAccessToken") == null ||
                    AppProps.getProperty("twitterAccessToken").isEmpty() ||
                    AppProps.getProperty("twitterAccessTokenSecret") == null ||
                    AppProps.getProperty("twitterAccessTokenSecret").isEmpty()
                    ) {
                String ErrorMsg = "Not all twitter authentication keys were found on app properties file.";
                LOGGER.log(Level.SEVERE, ErrorMsg);
                throw new UnsupportedOperationException(ErrorMsg);
            }


            if (AppProps.getProperty("elasticSearchServerName") == null ||
                    AppProps.getProperty("elasticSearchServerName").isEmpty() ||
                    AppProps.getProperty("elasticSearchPortNo") == null ||
                    AppProps.getProperty("elasticSearchPortNo").isEmpty()
                    ) {
                String ErrorMsg = "Invalid datastore connection parameters specified.";
                LOGGER.log(Level.SEVERE, ErrorMsg);
                throw new UnsupportedOperationException(ErrorMsg);
            }
        }
        catch(IOException ex)
        {
            LOGGER.log(Level.SEVERE, "Couldn't get app properties from file.", ex);
            throw new RuntimeException(ex);
        }
    }
}
