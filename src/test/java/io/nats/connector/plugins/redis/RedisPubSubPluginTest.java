// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector.plugins.redis;

import io.nats.connector.Connector;
import org.junit.Test;
import org.junit.Assert;
import io.nats.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by colinsullivan on 12/31/15.
 */
public class RedisPubSubPluginTest {

    static final int TESTCOUNT = 5;
    static final String REDIS_PAYLOAD = "Hello from Redis!";
    static final String NATS_PAYLOAD = "Hello from NATS!";


    static final Logger logger = LoggerFactory.getLogger(RedisPubSubPluginTest.class);
    @Test

    public void testRun() throws Exception {

        System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.redis.RedisPubSubPluginTest", "trace");

        System.setProperty(Connector.USER_PROP_PLUGIN_CLASS, RedisPubSubPlugin.class.getName());
        System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.redis.RedisPubSubPlugin", "trace");
        //System.setProperty("org.slf4j.simpleLogger.log.io.nats.client", "trace");
        //System.setProperty(RedisPubSubPlugin.CONFIG_URL, "file:///Users/colinsullivan/redis_nats_connector.json");


        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new RedisClient());
        executor.execute(new NATSClient());

        new Connector().run();

        logger.info("Test exiting.");

    }

    class RedisClient extends JedisPubSub implements Runnable
    {

        Jedis pubJedis;
        Jedis subJedis;
        JedisPool jedisPool = new JedisPool();
        int receivedCount = 0;

        @Override
        public void onMessage(String channel, String message)
        {
            logger.debug("Redis Client:  Received message: {}", message);

            Assert.assertTrue(NATS_PAYLOAD.equals(message));

            pubJedis.publish("Export_NATS", REDIS_PAYLOAD);

            if ((++receivedCount) == TESTCOUNT)
            {
                logger.info("Redis Client:  Completed.");
                subJedis.close();
            }
        }

        @Override
        public void run() {

            logger.info("Redis Client:  Starting");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            subJedis = jedisPool.getResource();
            pubJedis = jedisPool.getResource();

            Jedis j = new Jedis();
            j.connect();
            for (int i = 0; i < TESTCOUNT; i++) {
                j.subscribe(this, "Import_NATS");
            }

            logger.info("Redis Client:  Exiting.");


        }
    }

    class NATSClient implements Runnable, MessageHandler
    {

        boolean isComplete = false;
        Object  completeLock = new Object();

        @Override
        public void run() {

            try {

                logger.info("NATS Client:  Starting");


                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                io.nats.client.Connection c = new ConnectionFactory().createConnection();
                AsyncSubscription s = c.subscribeAsync("Import.Redis", this);
                s.start();

                for (int i = 0; i < TESTCOUNT; i++)
                {
                    c.publish("Export.Redis", NATS_PAYLOAD.getBytes());
                }
                c.flush();

                synchronized (completeLock) {
                    while (!isComplete) {
                        completeLock.wait();
                    }
                }

                logger.info("NATS Client:  Exiting.");

            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }

        }

        int receivedCount = 1;

        @Override
        public void onMessage(Message message) {

            String value = new String (message.getData());

            org.junit.Assert.assertTrue(REDIS_PAYLOAD.equals(value));

            if ((++receivedCount) == TESTCOUNT)
            {
                logger.debug("NATSClient Received {} messages.  Shutting down.");
                synchronized (completeLock) {
                    isComplete = true;
                    completeLock.notify();
                }
            }
        }
    }
}