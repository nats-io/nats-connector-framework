package io.nats.connector.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.nats.client.Message;
import io.nats.connector.plugins.NATSConnector;
import io.nats.connector.plugins.NATSConnectorPlugin;
import io.nats.connector.plugins.NATSEvent;
import org.slf4j.Logger;

/**
 * Created by colinsullivan on 12/16/15.
 */
public class TestPlugin implements NATSConnectorPlugin {

    NATSConnector connector = null;
    Logger logger = null;

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
    public String getName()
    {
        return "NATS Test Plugin";
    }

    @Override
    public boolean OnStartup(Logger logger) {
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

    public void OnNATSEvent(NATSEvent event, String message)
    {
        switch (event)
        {
            case ASYNC_ERROR:
                logger.error("Received async error:" + message);
                break;
            case RECONNECTED:
                logger.info("Reconnected.");
                break;
            case DISCONNECTED:
                logger.info("Disconnected.");
                break;
            default:
        }

    }
}
