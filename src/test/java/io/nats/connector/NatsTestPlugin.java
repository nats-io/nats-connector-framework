// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector;

import io.nats.client.Message;
import io.nats.client.ConnectionFactory;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by colinsullivan on 12/30/15.
 */
public class NatsTestPlugin implements NATSConnectorPlugin  {
    NATSConnector connector = null;
    Logger logger = null;

    public NatsTestPlugin() {}

    class PeriodicSender implements Runnable
    {
        @Override
        public void run()
        {
            String s;

            Message m = new Message();

            m.setSubject("foo");
            m.setReplyTo("bar");

            for (int i = 0; i < 2; i++)
            {
                s = new String("Message-" + Integer.toString(i));

                byte[] payload = s.getBytes();

                m.setData(payload, 0, payload.length);

                connector.publish(m);

                try {
                    connector.flush();
                }
                catch (Exception e){
                    logger.error("Error with flush:  ", e);
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
            }

            // test the shutdown command.
            connector.shutdown();
            logger.info("Shutdown the NATS connect from the plugin.");
        }
    }

    @Override
    public boolean OnStartup(Logger logger, ConnectionFactory factory) {
        this.logger = logger;
        return true;
    }

    @Override
    public boolean OnNatsInitialized(NATSConnector connector)
    {
        this.connector = connector;

        logger.info("Starting up.");

        try
        {
            connector.subscribe("foo");
        }
        catch (Exception ex)
        {
            logger.error("Unable to subscribe: ", ex);
            return false;
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new PeriodicSender());

        return true;
    }

    @Override
    public void OnShutdown()
    {
        logger.info("Shutting down.");
    }

    @Override
    public void OnNATSMessage(io.nats.client.Message msg)
    {

        logger.info("Received message: " + msg.toString());

        msg.setSubject("baz");

        byte[] reply = "reply".getBytes();

        msg.setData(reply, 0, (int)reply.length);

        connector.publish(msg);

        try {
            connector.flush();
            logger.info("Flushed.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void OnNATSEvent(NATSEvent event, String message)
    {
        switch (event)
        {
            case ASYNC_ERROR:
                logger.error("NATS Event Async error: " + message);
                break;
            case RECONNECTED:
                logger.info("NATS Event Reconnected: " + message);
                break;
            case DISCONNECTED:
                logger.info("NATS Event Disconnected: " + message);
                break;
            case CLOSED:
                logger.info("NATS Event Closed: " + message);
                break;
            default:
                logger.info("NATS Event Unrecognized event: " + message);
        }
    }
}
