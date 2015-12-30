package io.nats.connector.plugins;

import java.util.Properties;
import org.slf4j.Logger;
import io.nats.client.ConnectionFactory;

/**
 * Created by colinsullivan on 12/16/15.
 */
public interface NATSConnectorPlugin {

    /**
     * Gets the name of this plugin for logging purposes.
     *
     * If null is returned, then the class
     * name is used.
     *
     * @return Name of the plug-in.
     */
    public String  getName();

    /**
     * Invoked when the connector is started up.
    *
     * @param logger - logger for the NATS connector process.
     * @return - true if the connector should continue, false otherwise.
     */
    public boolean OnStartup(Logger logger);

    /**
     * Invoked when the Plugin is shutting down.
     */
    public void OnShutdown();

    /**
     * Invoked when the NATS plug-in is ready to start receiving
     * and sending messages.  Subscribe and publish calls can be
     * made after this call is invoked.
     *
     * @param connector
     *
     * @return true if the plugin can continue.
     */
    public boolean OnNatsInitialized(NATSConnector connector);

    /**
     * Invoked anytime a NATS message is received to be processed.
     * @param msg - NATS message recieved.
     */
    public void OnNATSMessage(io.nats.client.Message msg);

    /**
     * Invoked anytime a NATS event occurs around a connection
     * or error, alerting the plugin to take appropriate action.
     *
     * For example, when reconnecting, buffer or pause incoming
     * data to be sent to NATS.
     *
     * @param event
     * @param message
     */
    public void OnNATSEvent(NATSEvent event, String message);
}
