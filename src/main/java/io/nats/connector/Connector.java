// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector;

import io.nats.connector.plugin.NATSConnectorPlugin;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;


/**
 * This is the main connector class for the NATS Connector.
 * It operates as follows:
 *
 * It is entirely java properties driven - these can be set as parameters
 * to the JVM or passed in as a file.
 *
 * The Connector starts a thread which drives a DataFlowHandler.
 * The handler connects to NATS and invokes various interfaces on
 * a supplied plugin.
 *
 * The plugin has a few APIs that allow it to publish messages,
 * flush the NATS connection, and subscribe to various subjects.
 *
 * A plugin can both subscribe to receive data and export it to another
 * system, feed data into NATS, or both.
 *
 * The plugin's responsibilities include:
 *
 * Ensuring performance out of nats.  This may include some buffering
 * if the destination of the data consumer slower than NATS produces it.
 *
 * Translation of external origin/destination and the subject namespace.
 */
public class Connector implements Runnable
{
    /**
     * Name of the property to set the plugin class name.
     */
    static public final String USER_PROP_PLUGIN_CLASS = "com.io.nats.connector.plugin";

    static final Logger logger = LoggerFactory.getLogger(Connector.class);

    NATSConnectorPlugin plugin = null;
    Properties          gwProps = null;
    String              configFile = null;
    DataFlowHandler     flowHandler = null;


    private NATSConnectorPlugin loadPlugin(String className) throws
            ClassNotFoundException, InstantiationException,
            IllegalAccessException
    {
        logger.debug("Loading plugin: " + className);

        try {
            return (NATSConnectorPlugin)Class.forName(className).newInstance();
        }
        catch (ClassNotFoundException cnfe) {
            logger.error("Unable to find class " + className);
            logger.debug("Exception: ", cnfe);
            throw cnfe;
        }
        catch (InstantiationException ie)
        {
            logger.error("Unable to instantiate class " + className);
            logger.debug("Exception: ", ie);
            throw ie;
        }
        catch (IllegalAccessException iae)
        {
            logger.error("Illegal access of class " + className);
            logger.debug("Exception: ", iae);
            throw iae;
        }
    }

    @Override
    public void run()
    {
        try
        {
            logger.info("NATS Connector starting up.");

            flowHandler = new DataFlowHandler(plugin, gwProps, logger);

            Runtime.getRuntime().addShutdownHook(
                    new Thread()
                    {
                        public void run()
                        {
                            logger.debug("Cleaning up from shutdown hook.");
                            flowHandler.cleanup();
                        }
                    });

            flowHandler.process();

            logger.info("NATS Connector has shut down.");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Shuts down a running Connector.
     */
    public void shutdown()
    {
        flowHandler.shutdown();
    }

    private void traceProperties()
    {
        logger.trace("Properties:");
        for (String s : gwProps.stringPropertyNames())
        {
            logger.trace("{}={}", s, gwProps.get(s));
        }
    }

    public Connector(String[] args) throws Exception
    {
        if (args != null)
            parseArgs(args);

        gwProps = getProperties();

        traceProperties();

        String className = gwProps.getProperty(USER_PROP_PLUGIN_CLASS);
        if (className == null)
        {
            logger.error("Required property " + USER_PROP_PLUGIN_CLASS + " is not set.");
            throw new Exception("Connector plugin class not set.");
        }

        plugin = loadPlugin(className);
    }

    public Connector() throws Exception {
        this(null);
    }

    private void usage()
    {

        System.out.printf("java {} -config <properties file>", Connector.class.toString());
        System.exit(-1);
    }

    private void parseArgs(String args[])
    {
        if (args == null)
            return;

        if (args.length == 0)
            return;

        if (args.length < 2)
            usage();

        // only one arg, so keep it simple
        if ("-config".equalsIgnoreCase(args[0]))
            configFile = args[1];
        else
            usage();
    }

    private Properties getProperties() throws Exception{

        // add those from the VM.
        Properties p = new Properties(System.getProperties());

        if (configFile == null)
            return p;

        logger.debug("Loading properties from '" + configFile + '"');
        FileInputStream in = new FileInputStream(configFile);
        try {
            p.load(in);
        }
        catch (Exception e) {
            logger.error("Unable to load properties.", e);
            throw e;
        }
        finally {
            in.close();
        }

        return p;
    }

    public static void main(String[] args)
    {
        try
        {
            // We could create and executor, etc, but just run it.  It'll add a
            // shutdown hook, etc.  The NATS connector can be run as a thread
            // from anywhere.
            new Connector(args).run();
        }
        catch (Exception e)
        {
            logger.error("Severe Error: ", e);
        }
    }
}
