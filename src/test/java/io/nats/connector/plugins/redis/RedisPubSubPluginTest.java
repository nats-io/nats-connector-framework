// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector.plugins.redis;

import io.nats.connector.Connector;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import io.nats.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisPubSubPluginTest {

    static final String REDIS_PAYLOAD = "Hello from Redis!";
    static final String NATS_PAYLOAD = "Hello from NATS!";

    Logger logger = null;

    abstract class TestClient
    {
        Object readyLock = new Object();
        boolean isReady = false;

        String id = "";

        Object completeLock = new Object();
        boolean isComplete = false;

        protected int testCount = 0;

        int msgCount = 0;

        int tallyMessage()
        {
            return (++msgCount);
        }

        int getMessageCount()
        {
            return msgCount;
        }

        TestClient(String id, int testCount)
        {
            this.id = id;
            this.testCount = testCount;
        }

        void setReady()
        {
            logger.debug("Client ({}) is ready.", id);
            synchronized (readyLock)
            {
                if (isReady)
                    return;

                isReady = true;
                readyLock.notifyAll();
            }
        }

        void waitUntilReady()
        {
            synchronized (readyLock)
            {
                while (!isReady) {
                    try {
                        readyLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            logger.debug("Done waiting for Client ({}) to be ready.", id);
        }

        void setComplete()
        {
            logger.debug("Client ({}) has completed.", id);

            synchronized(completeLock)
            {
                if (isComplete)
                    return;

                isComplete = true;
                completeLock.notifyAll();
            }
        }

        void waitForCompletion()
        {
            synchronized (completeLock)
            {
                while (!isComplete)
                {
                    try {
                        completeLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            logger.debug("Done waiting for Client ({}) to complete.", id);
        }
    }

    /**
     * Simulates a simple Redis publisher.
     */
    public class RedisPublisher extends TestClient implements Runnable
    {
        String channel;
        JedisPool jedisPool = new JedisPool();

        RedisPublisher(String id, String channel, int count)
        {
            super(id, count);
            logger.debug("Creating Redis Publisher ({})", id);
            this.channel = channel;
        }

        @Override
        public void run() {

            logger.debug("RedisPublisher:  {}  Starting.", id);
            Jedis jedis = jedisPool.getResource();
            jedis.connect();

            logger.debug("RedisPublisher:  {}  connected.", id);

            for (int i = 0; i < testCount; i++) {
                jedis.publish(channel, REDIS_PAYLOAD);
                tallyMessage();
            }

            logger.debug("Redis Publisher ({}) :  Published {} messages", id, testCount);

            jedis.disconnect();

            setComplete();
        }
    }

    /**
     * Simulates a simple Redis subscriber.
     */
    public class RedisSubscriber extends TestClient implements Runnable
    {
        boolean checkPayload = true;

        JedisPool jedisPool = new JedisPool();
        Jedis     jedis = null;
        String    channel = null;

        RedisSubscriber(String id, String channel, int count)
        {
            super(id, count);
            logger.debug("Creating Redis Subscriber ({})", id);
            this.channel = channel;
        }

        @Override
        void waitForCompletion() {
            super.waitForCompletion();
            jedis.disconnect();
        }

        class RedisListener extends JedisPubSub {
            @Override
            public void onMessage(String channel, String message) {
                logger.trace("Redis Subscriber ({}):  Received message: {}", id, message);

                if (checkPayload) {
                    Assert.assertTrue(NATS_PAYLOAD.equals(message));
                }

                if (tallyMessage() == testCount) {
                    logger.debug("Redis Subscriber ({}):  Received {} messages.  Completed.", id, testCount);
                    this.unsubscribe();
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                setReady();
                super.onSubscribe(channel, subscribedChannels);
            }

            @Override
            public void onPSubscribe(String pattern, int subscribedChannels) {
                setReady();
                super.onPSubscribe(pattern, subscribedChannels);
            }

        }

        @Override
        public void run() {

            RedisListener rl = new RedisListener();

            jedis = jedisPool.getResource();
            jedis.connect();

            logger.trace("Redis Subscriber ({}):  subscribing to {}.", id, channel);

            jedis.subscribe(rl, channel);

            logger.debug("Redis Subscriber ({}):  Exiting.", id);

            setComplete();
        }
    }

    /**
     * Simulates a simple NATS publisher.
     */
    class NatsPublisher extends TestClient implements Runnable
    {
        String subject = null;

        NatsPublisher(String id, String subject, int count)
        {
            super(id, count);
            this.subject = subject;

            logger.debug("Creating NATS Publisher ({})", id);
        }

        @Override
        public void run() {

            try {

                logger.debug("NATS Publisher ({}):  Starting", id);

                io.nats.client.Connection c = new ConnectionFactory().createConnection();

                for (int i = 0; i < testCount; i++) {
                    c.publish(subject, NATS_PAYLOAD.getBytes());
                    tallyMessage();
                }
                c.flush();

                logger.debug("NATS Publisher ({}):  Published {} messages.", id, testCount);

                setComplete();
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Simulates a simple NATS subscriber.
     */
    class NatsSubscriber extends TestClient implements Runnable, MessageHandler
    {
        String subject = null;
        boolean checkPayload = true;

        NatsSubscriber(String id, String subject, int count)
        {
            super(id, count);
            this.subject = subject;

            logger.debug("Creating NATS Subscriber ({})", id);
        }

        @Override
        public void run() {

            try {
                logger.trace("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject);

                io.nats.client.Connection c = new ConnectionFactory().createConnection();

                AsyncSubscription s = c.subscribeAsync(subject, this);
                s.start();

                setReady();

                logger.debug("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject);

                waitForCompletion();

                s.unsubscribe();

                logger.debug("NATS Subscriber ({}):  Exiting.", id);
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }

        @Override
        public void onMessage(Message message) {

            String value = new String (message.getData());

            logger.trace("NATS Subscriber ({}):  Received message: {}", id, value);

            if (checkPayload) {
                org.junit.Assert.assertTrue(REDIS_PAYLOAD.equals(value));
            }

            if (tallyMessage() == testCount)
            {
                logger.debug("NATS Subscriber ({}) Received {} messages.  Completed.", id, testCount);
                setComplete();
            }
        }
    }

    private String generateContentFile(String content) throws Exception
    {
        File workingFile = new File("currentRedisConfig.json");
        BufferedWriter bw = new BufferedWriter(new FileWriter(workingFile));
        bw.write(content);
        bw.close();

        return workingFile.toURI().toString();
    }

    @Before
    public void initialize()
    {
        System.setProperty(Connector.USER_PROP_PLUGIN_CLASS, RedisPubSubPlugin.class.getName());

        // Enable tracing for debugging as necessary.
        //System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.redis.RedisPubSubPlugin", "trace");
        //System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.redis.RedisPubSubPluginTest", "trace");
        //System.setProperty("org.slf4j.simpleLogger.log.io.nats.client", "trace");

        logger = LoggerFactory.getLogger(RedisPubSubPluginTest.class);
    }

    public void testOneToOneWithDefaultConfig(int count) throws Exception {

        Connector c = new Connector();

        ExecutorService executor = Executors.newFixedThreadPool(6);

        RedisSubscriber rs = new RedisSubscriber("rs", "Import_NATS", count);
        RedisPublisher  rp = new RedisPublisher("rp", "Export_NATS",  count);

        NatsPublisher   np = new NatsPublisher("np", "Export.Redis",  count);
        NatsSubscriber  ns = new NatsSubscriber("ns", "Import.Redis", count);

        // start the connector
        executor.execute(c);

        // start the subsciber apps
        executor.execute(rs);
        executor.execute(ns);

        // wait for subscribers to be ready.
        rs.waitUntilReady();
        ns.waitUntilReady();

        // let the connector start
        Thread.sleep(1000);

        // start the publishers
        executor.execute(np);
        executor.execute(rp);

        // wait for the subscribers to complete.
        rs.waitForCompletion();
        ns.waitForCompletion();

        Assert.assertTrue("Invalid count", rs.getMessageCount() == count);
        Assert.assertTrue("Invalid count", ns.getMessageCount() == count);

        c.shutdown();
    }

    @Test
    public void testBasicOneToOne() throws Exception {
        testOneToOneWithDefaultConfig(5);
    }

    // @Test
    public void testBasicOneToOneStress() throws Exception {
        testOneToOneWithDefaultConfig(50000);
    }

    @Test
    public void testNATSExportWildcard() throws Exception {
        String wildcardConfiguration =
                "{" +
                        "\"nats_to_redis_map\" : [" +
                        "{" +
                        "\"subject\" : \"Export.*\"," +
                        "\"channel\" : \"Import_NATS\"" +
                        "}" +
                        "]," +
                        "\"redis_to_nats_map\" : [" +
                        "{" +
                        "\"channel\" : \"Export_NATS\"," +
                        "\"subject\" : \"Import.Redis\"" +
                        "}" +
                        "]" +
                        "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(wildcardConfiguration));

        // success means it won't hang.
        new Connector().run();
    }

    @Test
    public void testNATSImportWildcard() throws Exception {
        String wildcardConfiguration =
                "{" +
                        "\"nats_to_redis_map\" : [" +
                        "{" +
                        "\"subject\" : \"Export.Redis\"," +
                        "\"channel\" : \"Import_NATS\"" +
                        "}" +
                        "]," +
                        "\"redis_to_nats_map\" : [" +
                        "{" +
                        "\"channel\" : \"Export_NATS\"," +
                        "\"subject\" : \"Import.*\"" +
                        "}" +
                        "]" +
                        "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(wildcardConfiguration));

        // success means it won't hang.
        new Connector().run();
    }

    @Test
    public void testCircularRouteDetection() throws Exception {
        String wildcardConfiguration =
                "{" +
                        "\"nats_to_redis_map\" : [" +
                        "{" +
                        "\"subject\" : \"foo\"," +
                        "\"channel\" : \"NATS\"" +
                        "}" +
                        "]," +
                        "\"redis_to_nats_map\" : [" +
                        "{" +
                        "\"channel\" : \"NATS\"," +
                        "\"subject\" : \"foo\"" +
                        "}" +
                        "]" +
                        "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(wildcardConfiguration));

        Connector c = new Connector();

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(c);
        Thread.sleep(1000);
        c.shutdown();

        // TODO - check logs for circular route message.
    }

    @Test
    public void testMulipleMappings() throws Exception {

        int count = 5;

        String config =
                "{" +
                        "\"nats_to_redis_map\" : [" +
                        "{" +
                        "\"subject\" : \"Export.Redis\"," +
                        "\"channel\" : \"Import_NATS\"" +
                        "}," +
                        "{" +
                        "\"subject\" : \"Export.Redis2\"," +
                        "\"channel\" : \"Import_NATS2\"" +
                        "}" +
                        "]," +
                        "\"redis_to_nats_map\" : [" +
                        "{" +
                        "\"channel\" : \"Export_NATS\"," +
                        "\"subject\" : \"Import.Redis\"" +
                        "}," +
                        "{" +
                        "\"channel\" : \"Export_NATS2\"," +
                        "\"subject\" : \"Import.Redis2\"" +
                        "}" +
                        "]" +
                        "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(config));

        Connector c = new Connector();

        ExecutorService executor = Executors.newFixedThreadPool(10);

        RedisSubscriber rs1 = new RedisSubscriber("rs1", "Import_NATS", count);
        RedisPublisher  rp1 = new RedisPublisher("rp1", "Export_NATS",  count);
        RedisSubscriber rs2 = new RedisSubscriber("rs2", "Import_NATS2", count);
        RedisPublisher  rp2 = new RedisPublisher("rp2", "Export_NATS2",  count);

        NatsPublisher   np1 = new NatsPublisher("np1", "Export.Redis",  count);
        NatsSubscriber  ns1 = new NatsSubscriber("ns1", "Import.Redis", count);
        NatsPublisher   np2 = new NatsPublisher("np2", "Export.Redis2",  count);
        NatsSubscriber  ns2 = new NatsSubscriber("ns2", "Import.Redis2", count);

        // start the connector
        executor.execute(c);

        // start the subsciber apps
        executor.execute(rs1);
        executor.execute(rs2);
        executor.execute(ns1);
        executor.execute(ns2);

        // wait for subscribers to be ready.
        rs1.waitUntilReady();
        rs2.waitUntilReady();
        ns1.waitUntilReady();
        ns2.waitUntilReady();

        // let the connector start
        Thread.sleep(1000);

        // start the publishers
        executor.execute(np1);
        executor.execute(np2);
        executor.execute(rp1);
        executor.execute(rp2);

        // wait for the subscribers to complete.
        rs1.waitForCompletion();
        rs2.waitForCompletion();
        ns1.waitForCompletion();
        ns2.waitForCompletion();

        Assert.assertTrue("Invalid count", rs1.getMessageCount() == count);
        Assert.assertTrue("Invalid count", rs2.getMessageCount() == count);
        Assert.assertTrue("Invalid count", ns1.getMessageCount() == count);
        Assert.assertTrue("Invalid count", ns2.getMessageCount() == count);

        c.shutdown();

    }

    /**
     * Test fanout NATS -> 2 Redis channels
     * Also tests one map present.
     * @throws Exception
     */
    @Test
    public void testNatsSubjectFanoutToRedis() throws Exception {

        int count = 5;

        String config =
                "{" +
                        "\"nats_to_redis_map\" : [" +
                        "{" +
                        "\"subject\" : \"Export.Redis\"," +
                        "\"channel\" : \"Import_NATS\"" +
                        "}," +
                        "{" +
                        "\"subject\" : \"Export.Redis\"," +
                        "\"channel\" : \"Import_NATS2\"" +
                        "}" +
                        "]" +
                        "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(config));

        Connector c = new Connector();

        ExecutorService executor = Executors.newFixedThreadPool(10);

        RedisSubscriber rs1 = new RedisSubscriber("rs1", "Import_NATS", count);
        RedisSubscriber rs2 = new RedisSubscriber("rs2", "Import_NATS2", count);

        NatsPublisher   np1 = new NatsPublisher("np1", "Export.Redis",  count);

        // start the connector
        executor.execute(c);

        // start the subsciber apps
        executor.execute(rs1);
        executor.execute(rs2);

        // wait for subscribers to be ready.
        rs1.waitUntilReady();
        rs2.waitUntilReady();

        // let the connector start
        Thread.sleep(1000);

        // start the publishers
        executor.execute(np1);

        // wait for the subscribers to complete.
        rs1.waitForCompletion();
        rs2.waitForCompletion();

        Assert.assertTrue("Invalid count", rs1.getMessageCount() == count);
        Assert.assertTrue("Invalid count", rs2.getMessageCount() == count);

        c.shutdown();
    }

    @Test
    public void testRedisSubjectFanoutToNats() throws Exception {

        int count = 5;

        String config =
                "{" +
                        "\"redis_to_nats_map\" : [" +
                        "{" +
                        "\"channel\" : \"Export_NATS\"," +
                        "\"subject\" : \"Import.Redis\"" +
                        "}," +
                        "{" +
                        "\"channel\" : \"Export_NATS\"," +
                        "\"subject\" : \"Import.Redis2\"" +
                        "}" +
                        "]" +
                        "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(config));

        Connector c = new Connector();

        ExecutorService executor = Executors.newFixedThreadPool(10);

        RedisPublisher  rp1 = new RedisPublisher("rp1", "Export_NATS",  count);

        NatsSubscriber  ns1 = new NatsSubscriber("ns1", "Import.Redis", count);
        NatsSubscriber  ns2 = new NatsSubscriber("ns2", "Import.Redis2", count);

        // start the connector
        executor.execute(c);

        // start the subsciber apps
        executor.execute(ns1);
        executor.execute(ns2);

        // wait for subscribers to be ready.
        ns1.waitUntilReady();
        ns2.waitUntilReady();

        // let the connector start
        Thread.sleep(1000);

        // start the publishers
        executor.execute(rp1);

        // wait for the subscribers to complete.
        ns1.waitForCompletion();
        ns2.waitForCompletion();

        Assert.assertTrue("Invalid count", ns1.getMessageCount() == count);
        Assert.assertTrue("Invalid count", ns2.getMessageCount() == count);

        c.shutdown();

    }

    @Test
    public void testRedisSubjectFanIntoNats() throws Exception {

        int count = 500;

        String config =
                "{" +
                        "\"redis_to_nats_map\" : [" +
                        "{" +
                        "\"channel\" : \"Export_NATS\"," +
                        "\"subject\" : \"Import.Redis\"" +
                        "}," +
                        "{" +
                        "\"channel\" : \"Export_NATS2\"," +
                        "\"subject\" : \"Import.Redis\"" +
                        "}," +
                        "{" +
                        "\"channel\" : \"Export_NATS3\"," +
                        "\"subject\" : \"Import.Redis\"" +
                        "}" +
                        "]" +
                        "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(config));

        Connector c = new Connector();

        ExecutorService executor = Executors.newFixedThreadPool(10);

        RedisPublisher rp1 = new RedisPublisher("rp1", "Export_NATS",  count);
        RedisPublisher rp2 = new RedisPublisher("rp2", "Export_NATS2",  count);
        RedisPublisher rp3 = new RedisPublisher("rp3", "Export_NATS3",  count);

        NatsSubscriber ns1 = new NatsSubscriber("ns1", "Import.Redis", count*3);

        // start the connector
        executor.execute(c);

        // start the subsciber apps
        executor.execute(ns1);

        // wait for subscribers to be ready.
        ns1.waitUntilReady();

        // let the connector start
        Thread.sleep(1000);

        // start the publishers
        executor.execute(rp1);
        executor.execute(rp2);
        executor.execute(rp3);

        // wait for the subscribers to complete.
        ns1.waitForCompletion();

        Assert.assertTrue("Invalid count", ns1.getMessageCount() == (count*3));

        c.shutdown();

    }

    @Test
    public void testNatsSubjectFanIntoRedis() throws Exception {

        int count = 5;

        String config =
                "{" +
                        "\"nats_to_redis_map\" : [" +
                        "{" +
                        "\"subject\" : \"Export.Redis1\"," +
                        "\"channel\" : \"Import_NATS\"" +
                        "}," +
                        "{" +
                        "\"subject\" : \"Export.Redis2\"," +
                        "\"channel\" : \"Import_NATS\"" +
                        "}," +
                        "{" +
                        "\"subject\" : \"Export.Redis3\"," +
                        "\"channel\" : \"Import_NATS\"" +
                        "}" +
                        "]" +
                "}";


        System.setProperty(RedisPubSubPlugin.CONFIG_URL,
                generateContentFile(config));

        Connector c = new Connector();

        ExecutorService executor = Executors.newFixedThreadPool(10);

        NatsPublisher np1 = new NatsPublisher("np1", "Export.Redis1",  count);
        NatsPublisher np2 = new NatsPublisher("np2", "Export.Redis2",  count);
        NatsPublisher np3 = new NatsPublisher("np3", "Export.Redis3",  count);

        RedisSubscriber rs1 = new RedisSubscriber("rs1", "Import_NATS", count*3);

        // start the subsciber apps
        executor.execute(rs1);

        // wait for subscribers to be ready.
        rs1.waitUntilReady();

        // start the connector
        executor.execute(c);
        Thread.sleep(1000);;

        // start the publishers
        executor.execute(np1);
        executor.execute(np2);
        executor.execute(np3);

        // wait for the subscribers to complete.
        rs1.waitForCompletion();

        Assert.assertTrue("Invalid count", rs1.getMessageCount() == (count*3));

        c.shutdown();

    }
}