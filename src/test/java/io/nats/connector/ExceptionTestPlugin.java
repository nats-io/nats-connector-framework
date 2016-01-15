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
 * WIP
 */
public class ExceptionTestPlugin implements NATSConnectorPlugin  {
    NATSConnector connector = null;
    Logger logger = null;

    public ExceptionTestPlugin() {}

    class SendOneMessage implements Runnable
    {
        @Override
        public void run()
        {
            String s;


            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {}

            Message m = new Message();

            m.setSubject("foo");
            m.setReplyTo("bar");


            s = new String("payload");

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
            } catch (InterruptedException e) {}

            // test the shutdown command.
            connector.shutdown();
            logger.info("Shutdown the NATS connect from the plugin.");
        }
    }

    @Override
    public boolean onStartup(Logger logger, ConnectionFactory factory) {
        this.logger = logger;
        throw new RuntimeException("Exception from OnStartup");
    }

    @Override
    public boolean onNatsInitialized(NATSConnector connector)
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
        executor.execute(new SendOneMessage());

        throw new RuntimeException("Exception from OnNatsInitialized");
    }

    @Override
    public void onShutdown()
    {
        throw new RuntimeException("Exception from OnNATSMessage");
    }

    @Override
    public void onNATSMessage(Message msg)
    {
        throw new RuntimeException("Exception from OnNATSMessage");
    }

    @Override
    public void onNATSEvent(NATSEvent event, String message)
    {
        throw new RuntimeException("Exception from OnNatsEvent");
    }
}
