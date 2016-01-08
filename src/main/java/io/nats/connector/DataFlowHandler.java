// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector;

import io.nats.client.*;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import io.nats.connector.plugin.NATSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFlowHandler implements MessageHandler, NATSConnector {

    private NATSConnectorPlugin plugin     = null;

    // Rely heavily on NATS locking, but protect data structures here
    // with the plugin lock.
    private Object              pluginLock   = new Object();
    private HashMap             subscriptions = new HashMap<String, Subscription>();

    private Properties          properties = null;
    private Logger              logger     = null;

    private Object              runningLock = new Object();

    private Object cleanupLock = new Object();
    private boolean hasCleanedUp = false;

    // TODO eval - this for performance - preoptimization is evil.
    private AtomicBoolean     isRunning   = new AtomicBoolean();

    private ConnectionFactory connectionFactory = null;
    private Connection        connection        = null;

    public DataFlowHandler(NATSConnectorPlugin plugin, Properties props, Logger logger)
    {
        this.plugin = plugin;
        this.properties = props;
        this.logger = logger;
    }

    // TODO:  pass through to allow users to get their own?
    class EventHandlers implements ClosedEventHandler, DisconnectedEventHandler,
            ExceptionHandler, ReconnectedEventHandler
    {
        public void onReconnect(ConnectionEvent event)
        {
            try {
                String desc = "NATS Connection reconnected.";
                logger.info(desc);
                plugin.onNATSEvent(NATSEvent.RECONNECTED, desc);
            }
            catch (Exception e) {
                logger.error("Runtime exception in plugin method OnNATSEvent (RECONNECTED): ", e);
            }
        }

        public void onClose(ConnectionEvent event)
        {
            try {
                plugin.onNATSEvent(NATSEvent.CLOSED, "NATS Connection closed.");
            }
            catch (Exception e) {
                logger.error("Runtime exception in plugin method OnNATSEvent (CLOSED): ", e);
            }
        }

        public void onException(Connection conn, Subscription sub, Throwable ex)
        {
            try {
                logger.error("Asynchronous error: subject: {} exception: {}",
                        sub.getSubject(), ex.getMessage());

                plugin.onNATSEvent(NATSEvent.ASYNC_ERROR, ex.getMessage());
            }
            catch (Exception e) {
                logger.error("Runtime exception in plugin method OnNATSEvent (CLOSED): ", e);
            }
        }

        public void onDisconnect(ConnectionEvent event) {
            try {

                String desc = "NATS Connection disconnected.";
                logger.info(desc);

                plugin.onNATSEvent(NATSEvent.DISCONNECTED, desc);
            }
            catch (Exception e) {
                logger.error("Runtime exception in plugin method OnNATSEvent (CLOSED): ", e);
            }
        }
    }

    private void setup() throws Exception
    {
        connectionFactory = new ConnectionFactory(properties);
        EventHandlers eh = new EventHandlers();

        connectionFactory.setClosedEventHandler(eh);
        connectionFactory.setDisconnectedEventHandler(eh);
        connectionFactory.setExceptionHandler(eh);
        connectionFactory.setReconnectedEventHandler(eh);

        // invoke on startup here, so the user can override or set their
        // own callbacks in the plugin if need be.
        if (invokeOnStartup(connectionFactory) == false) {
            shutdown();
            throw new Exception("Startup failure initiated From plug-in");
        }

        connection = connectionFactory.createConnection();
        logger.debug("Connected to NATS cluster.");
    }

    private void teardown()
    {
        try
        {
            if (subscriptions != null)
            {
                for (Object s : subscriptions.values())
                    ((Subscription) s).unsubscribe();

                subscriptions.clear();
                subscriptions = null;
            }
        }
        catch (Exception e) {}

        try
        {
            if (connection != null)
                 connection.close();
        }
        catch (Exception e) {}

        logger.debug("Closed connection to NATS cluster.");
    }

    /*
     *   TODO:  How well can request reply work?  Need additional support?
     */
    public void onMessage(Message m)
    {
        try
        {
            plugin.onNATSMessage(m);
        }
        catch (Exception e)
        {
            logger.error("Runtime exception thrown by plugin (onMessage): ", e);
        }
    }

    private boolean invokeOnStartup(ConnectionFactory factory)
    {

        logger.debug("OnStartup");
        try
        {
            return plugin.onStartup(LoggerFactory.getLogger(plugin.getClass().getName()), factory);
        }
        catch (Exception e)
        {
            logger.error("Runtime exception thrown by plugin (OnStartup): ", e);
            return false;
        }
    }

    private boolean invokeOnNatsInitialized()
    {
        logger.debug("OnNatsInitialized");
        try
        {
            return plugin.onNatsInitialized(this);
        }
        catch (Exception e)
        {
            logger.error("Runtime exception thrown by plugin (OnNatsInitialized): ", e);
            return false;
        }
    }

    private void invokePluginShutdown()
    {
        logger.debug("OnShutdown");
        try
        {
            plugin.onShutdown();
        }
        catch (Exception e)
        {
            logger.error("Runtime exception thrown by plugin (OnShutdown): ", e);
        }
    }

    public void process()
    {
        logger.debug("Setting up NATS Connector.");

        boolean running = true;

        try {
            // connect to the NATS cluster
            setup();
        }
        catch (Exception e) {
            logger.error("Setup error: ", e);
            cleanup();
            return;
        }

        if (!invokeOnNatsInitialized())
        {
            logger.error("Plugin failed to start.  Exiting.");
            cleanup();
            return;
        }

        logger.info("The NATS Connector is running.");

        isRunning.set(true);

        while (running)
        {
            synchronized(runningLock)
            {
                try {
                    runningLock.wait();
                }
                catch (InterruptedException e) {
                    // As of java 1.6, Object.wait can be woken up spuriously,
                    // so we need to check if we are still running.
                }

                running = isRunning.get();
            }
        }

        cleanup();
    }

    public void cleanup()
    {
        synchronized (cleanupLock)
        {
            if (hasCleanedUp)
                return;


            logger.info("Cleaning up.");

            // we are shutting down.
            invokePluginShutdown();
            teardown();

            hasCleanedUp = true;
        }

        logger.debug("Cleaned up NATS Connector.");
    }

    /*
     * NATSConnector
     */
    public void publish(Message msg)
    {
        if (isRunning.get() == false)
            return;

        connection.publish(msg);
    }

    public void flush() throws Exception
    {
        // if the connector is shutting down, then we silently fail.
        if (isRunning.get() == false)
            return;

        if (connection == null)
            throw new Exception("Invalid state.  Connection is null.");

        try {
            connection.flush();
        }
        catch (Exception ex)
        {
            throw new Exception("Unable to flush NATS connection.", ex);
        }
    }

    public void shutdown()
    {
        if (isRunning.get() == false)
            return;

        logger.debug("Plugin initiated NATS connector shutdown.");

        isRunning.set(false);

        synchronized (runningLock)
        {
            runningLock.notify();
        }
    }

    public void subscribe(String subject, MessageHandler handler)
    {
        if (subject == null)
            return;

        synchronized (pluginLock)
        {
            logger.debug("Plugin subscribe to '{}'.", subject);

            // do not subscribe twice.
            if (subscriptions.containsKey(subject)) {
                logger.debug("Subscription already exists.");
                return;
            }

            AsyncSubscription s = connection.subscribeAsync(subject, handler);
            subscriptions.put(subject, s);

            logger.debug("Subscribed.");
        }
    }

    public void subscribe(String subject)
    {
        subscribe(subject, this);
    }

    public void subscribe(String subject, String queue, MessageHandler handler) throws Exception {

        if (subject == null || queue == null)
            return;

        synchronized (pluginLock)
        {
                logger.debug("Plugin subscribe to '{}', queue '{}'.", subject,
                    (queue == null ? "(none)" : queue));

            // do not subscribe twice.
            if (subscriptions.containsKey(subject)) {
                logger.debug("Subscription already exists.");
                return;
            }

            AsyncSubscription s;

            if (queue != null)
                s = connection.subscribeAsync(subject, handler);
            else
                s = connection.subscribeAsync(subject, queue, handler);

            s.start();

            subscriptions.put(subject, s);
        }
    }

    public void subscribe(String subject, String queue) throws Exception {
        subscribe(subject, queue, null);
    }

    public void unsubscribe(String subject)
    {
        if (subject == null)
            return;

        synchronized (pluginLock)
        {
            logger.debug("Plugin unsubscribe from '{}'.", subject);

            Subscription s = (Subscription) subscriptions.get(subject);
            if (s == null) {
                logger.debug("Subscription not found.");
                return;
            }

            try {
                s.unsubscribe();
            } catch (Exception e) {
                logger.debug("Plugin unsubscribe failed.", e);
                return;
            }
        }
    }

    public Connection getConnection()
    {
        return connection;
    }

    public ConnectionFactory getConnectionFactory()
    {
        return connectionFactory;
    }

}
