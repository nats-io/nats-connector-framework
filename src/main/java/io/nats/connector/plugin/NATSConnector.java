package io.nats.connector.plugin;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.MessageHandler;

/**
 * Created by colinsullivan on 12/16/15.
 */
public interface NATSConnector {

    /**
     * In case of a critical failure or security issue, this allows the plugin
     * to request a shutdown of the connector.
     */
    public void shutdown();

    /***
     * Publishes a message into the NATS cluster.
     *
     * @param message - the message to publish.
     */
    public void publish(io.nats.client.Message message);

    /***
     * Flushes any pending NATS data.
     */
    public void flush() throws Exception;

    /***
     * Adds interest in a NATS subject.
     */
    public void subscribe(String subject) throws Exception;

    /***
     * Adds interest in a NATS subject, with a custom handler.
     */
    public void subscribe(String subject, MessageHandler handler) throws Exception;

    /***
     * Adds interest in a NATS subject with a queue group.
     */
    public void subscribe(String subject, String queue) throws Exception;

    /***
     * Adds interest in a NATS subject with a queue group, with a custom handler.
     */
    public void subscribe(String subject, String queue, MessageHandler handler) throws Exception;

    /***
     * Removes interest in a NATS subject
     * @param subject
     */
    public void unsubscribe(String subject);

    /***
     * Advanced API to get the NATS connection
     */
    public Connection getConnection();

    /***
     * Advanced API to get the Connection Factory
     */
    public ConnectionFactory getConnectionFactory();


}
