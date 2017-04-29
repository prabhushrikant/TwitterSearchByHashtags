package com.experticity.twitterAssignment;

import java.io.IOException;
import java.util.logging.*;
/**
 * Created by shrikant on 4/22/17.
 * Logger class for twitterApp application.
 */
public class twitterLogger {

    private static final Logger LOGGER = Logger.getLogger(twitterLogger.class.getName());

    //Creating consoleHandler and fileHandler
    Handler consoleHandler = null;
    Handler logFileHandler  = null;

    public  void initialize() {

        try {
            consoleHandler = new ConsoleHandler();
            logFileHandler = new FileHandler("./twitterApp.log");

            //Assigning handlers to LOGGER object
            LOGGER.addHandler(consoleHandler);
            LOGGER.addHandler(logFileHandler);

            LOGGER.log(Level.INFO, "Successfully created log file.");
        }
        catch(IOException ex)
        {
            LOGGER.log(Level.SEVERE, "Error while creating log file.", ex);
        }
    }
}
