package com.experticity.twitterAssignment;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;
/**
 * Created by shrikant on 4/22/17.
 * Logger class for twitterApp application.
 */
public class twitterLogger {

    private static final Logger LOGGER = Logger.getLogger(twitterLogger.class.getName());

    private static final String LOG_FILE_NAME = "./twitterApp1.log";

    //Creating consoleHandler and fileHandler
    Handler consoleHandler = null;
    Handler logFileHandler  = null;

    public twitterLogger()
    {
        initialize();
    }

    private  void initialize() {

        try {
            consoleHandler = new ConsoleHandler();
            File logfile = new File(LOG_FILE_NAME);
            if (logfile.exists())
            {
                //delete the old log file first
                logfile.delete();
            }
            logFileHandler = new FileHandler(LOG_FILE_NAME, true);

            //Assigning handlers to LOGGER object
            LOGGER.addHandler(consoleHandler);
            LOGGER.addHandler(logFileHandler);

            //Assigning formatter to the LOGGER object
            SimpleFormatter formatter = new SimpleFormatter();
            logFileHandler.setFormatter(formatter);

            LOGGER.log(Level.INFO, "Successfully created log file.");
        }
        catch(IOException ex)
        {
            LOGGER.log(Level.SEVERE, "Error while creating log file.", ex);
        }
    }
}
