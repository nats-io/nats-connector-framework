// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        throw new RuntimeException("This is an exception from onStartup");
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

        throw new RuntimeException("This is an exception from onNatsInitialized");
    }

    @Override
    public void onShutdown()
    {
        throw new RuntimeException("This is an exception from onShutdown");
    }

    @Override
    public void onNATSMessage(Message msg)
    {
        throw new RuntimeException("This is an exception from onNATSMessage");
    }

    @Override
    public void onNATSEvent(NATSEvent event, String message)
    {
        throw new RuntimeException("Exception from OnNatsEvent");
    }
}
