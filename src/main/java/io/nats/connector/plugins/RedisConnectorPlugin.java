package io.nats.connector.plugins;

import io.nats.client.Message;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;
import org.slf4j.Logger;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.*;

import java.util.Properties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by colinsullivan on 12/30/15.
 */
public class RedisConnectorPlugin implements NATSConnectorPlugin  {
    NATSConnector connector = null;
    Logger logger = null;


    JedisPool    jedisPool      = null;
    BinaryJedis  publishJedis   = null;

    ListenForRedisUpdates listener = null;

    String host;
    int    port;
    int    timeout;

    static public final String REDIS_HOST = "nats.io.connector.redisplugin.host";
    static public final String REDIS_PORT = "nats.io.connector.redisplugin.port";
    static public final String REDIS_TIMEOUT = "nats.io.connector.redisplugin.timeout";
    static public final String REDIS_SUBJECT_PREFIX = "nats.io.connector.redisplugin.subjectprefix";

    String subjectPrefix = "redis";

    private void loadProperties()
    {
        Properties p = System.getProperties();

        host = p.getProperty(REDIS_HOST, "localhost");
        port = Integer.valueOf(p.getProperty(REDIS_PORT, "6379"));
        subjectPrefix = p.getProperty(REDIS_SUBJECT_PREFIX, "redisplugin");
        timeout = Integer.valueOf(p.getProperty(REDIS_TIMEOUT, "2000"));
    }

    // TODO:  Map subjects, provide prefix, etc.
    private byte[] getChannelFromSubject(String subject)
    {
        return subject.getBytes();
    }

    private String getSubjectFromChannel(String channel)
    {
        return channel;
    }

    private class JedisListener extends JedisPubSub
    {
        Message natsMessage = new Message();

        private void sendNatsMessage(String channelOrPattern, String message)
        {
            String subject = getSubjectFromChannel(channelOrPattern);

            natsMessage.setSubject(subject);

            byte[] payload = message.getBytes();
            natsMessage.setData(payload, 0, payload.length);

            connector.publish(natsMessage);

            logger.debug("Published Message Redis ({}) -> NATS ({})", channelOrPattern, subject);
        }

        public void onMessage(String channel, String message)
        {
            sendNatsMessage(channel, message);
        }

        public void onSubscribe(String channel, int subscribedChannels)
        {
            logger.debug("Redis subscripton to channel {}", channel, subscribedChannels);
        }

        public void onUnsubscribe(String channel, int subscribedChannels) {
            logger.debug("Redis unsubscribe from channel {}", channel, subscribedChannels);
        }

        public void onPSubscribe(String pattern, int subscribedChannels) {
            logger.debug("Redis pattern subscribe {}", pattern, subscribedChannels);
        }

        public void onPUnsubscribe(String pattern, int subscribedChannels) {
            logger.debug("Redis pattern unsubscribe {}", pattern, subscribedChannels);
        }

        public void onPMessage(String pattern, String channel,
                               String message) {
            sendNatsMessage(channel, message);
        }
    }

    private class ListenForRedisUpdates implements Runnable
    {
        private boolean running = true;
        private Object  runningLock = new Object();
        Jedis   listenJedis = null;

        private void setRunning(boolean value)
        {
            synchronized (runningLock)
            {
                running = value;
            }
        }

        private boolean isRunning()
        {
            synchronized (runningLock)
            {
                return running;
            }
        }

        @Override
        public void run()
        {
            listenJedis = jedisPool.getResource();
            while (isRunning())
            {
                // TODO - how to break when stopping...
                try {
                    listenJedis.subscribe(new JedisListener(), "foo");
                }
                catch (JedisConnectionException e)
                {
                    logger.error("Lost connection to the Redis server.  Exiting.");

                    // TODO:  retry logic?
                    shutdown();
                }
            }
        }

        public void shutdown()
        {
            setRunning(false);
            listenJedis.close();
        }
    }

    public RedisConnectorPlugin() {}

    private void initJedis()
    {
        jedisPool = new JedisPool(new JedisPoolConfig(), host, port, timeout);
        publishJedis = jedisPool.getResource();
    }

    private void teardownJedis()
    {
        if (listener != null)
            listener.shutdown();

        publishJedis.close();
        jedisPool.close();
    }

    @Override
    public boolean OnStartup(Logger logger) {
        this.logger = logger;

        loadProperties();

        initJedis();

        return true;
    }

    @Override
    public boolean OnNatsInitialized(NATSConnector connector)
    {
        this.connector = connector;

        // TODO: map subjects....  Development purposes only here.

        try {
            connector.subscribe("bar");
        }
        catch (Exception e)
        {
            logger.error("Unable to subscribe to bar.");
            return false;
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new ListenForRedisUpdates());

        return true;
    }

    @Override
    public void OnShutdown()
    {
        teardownJedis();
    }

    @Override
    public void OnNATSMessage(Message msg)
    {
        String subject = msg.getSubject();

        publishJedis.publish(getChannelFromSubject(subject), msg.getData());

        logger.debug("Message NATS ({}) -> Redis", subject);
    }

    @Override
    public void OnNATSEvent(NATSEvent event, String message)
    {
    }
}
