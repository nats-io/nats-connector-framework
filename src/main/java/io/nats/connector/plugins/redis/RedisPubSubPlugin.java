// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector.plugins.redis;

import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;
import io.nats.connector.plugin.NATSUtilities;
import org.json.*;
import org.slf4j.Logger;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.*;

import java.util.Properties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.HashMap;
import java.util.Map.Entry;

/**
 * This is the Redis Pub/Sub connector plugin.
 *
 * It reads a configuration file from a provided url to direct
 * the connector to bridge NATS and Redis.
 *
 * The file is JSON has the following structure:
 * {
 *   "host" : "localhost",
 *   "port" : 6379,
 *   "timeout" : 2000,
 *   "nats_to_redis_map" : [
 *     {
 *       "subject" : "Export.Redis",
 *       "channel" : "Import_NATS"
 *     }
 *   ],
 *   "redis_to_nats_map" : [
 *     {
 *       "channel" : "Export_NATS",
 *       "subject" : "Import.Redis",
 *     }
 *   ]
 * }
 *
 * NATs publishing to Redis.Export will send messages to Redis on channel
 * NATS.import.
 *
 * Redis publishing to NATS.Export will send messages to NATS on subject
 * Redis.Import.
 *
 * This is highly customizable, by adding multiple subscriptions,
 * and supporting wildcard/pattern subscriptions.
 *
 * Take care to avoid circular routes generated
 * by overlapping maps should be avoided.
 *
 * Basic circular route detection is performed, but could be
 * easily achieved through NATS wildcarding or Redis patterns.
 *
 * While bad, it does provide a nice stress test for Redis.
 */
public class RedisPubSubPlugin implements NATSConnectorPlugin  {

    static public final String CONFIG_URL = "nats.io.connector.plugins.redispubsub.configurl";

    static public final String DEFAULT_REDIS_HOST = "localhost";
    static public final int DEFAULT_REDIS_PORT = 6379;
    static public final int DEFAULT_REDIS_TIMEOUT = 2000;

    NATSConnector connector = null;
    Logger logger = null;

    private HashMap<String, String> channelsToSubjects = null;
    private HashMap<String, String> subjectsToChannels = null;

    JedisPool    jedisPool      = null;
    BinaryJedis  publishJedis   = null;

    ListenForRedisUpdates listener = null;

    String host = DEFAULT_REDIS_HOST;
    int    port = DEFAULT_REDIS_PORT;
    int    timeout = DEFAULT_REDIS_TIMEOUT;

    String configUrl = null;

    String defaultConfiguration =
            "{" +
            "\"host\" : \"" + DEFAULT_REDIS_HOST + "\"," +
            "\"port\" : \""+ DEFAULT_REDIS_PORT + "\"," +
            "\"timeout\" : \"" + DEFAULT_REDIS_TIMEOUT + "\"," +
            "\"nats_to_redis_map\" : [" +
                    "{" +
                        "\"subject\" : \"Export.Redis\"," +
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


    private void loadProperties()
    {
        Properties p = System.getProperties();
        configUrl = p.getProperty(CONFIG_URL);
    }

    private void checkMapObj(JSONObject jo) throws Exception
    {
        if (jo.has("channel") == false)
            throw new Exception("channel not defined in map.");

        if (jo.has("subject") == false)
            throw new Exception("subject not defined in map.");
    }

    private void parseRedisToNatsMapObj(JSONObject destMapObj) throws Exception
    {
        checkMapObj(destMapObj);

        if (channelsToSubjects == null)
            channelsToSubjects = new HashMap<String, String>();

        String channel = destMapObj.getString("channel");
        String subject = destMapObj.getString("subject");

        logger.debug("Mapping Redis channel {} to NATS subject {}",
                channel, subject);

        channelsToSubjects.put(channel, subject);
    }

    private void parseNatsToRedisMapObj(JSONObject destMapObj) throws Exception
    {
        checkMapObj(destMapObj);

        if (subjectsToChannels == null)
            subjectsToChannels = new HashMap<String, String>();

        String channel = destMapObj.getString("channel");
        String subject = destMapObj.getString("subject");

        logger.debug("Mapping NATS subject {} to Redis channel {}",
                channel, subject);

        subjectsToChannels.put(subject, channel);
    }

    // Warn against circular routes; it won't end well.
    // TODO:  Wildcard/Pattern checks someday.
    private void warnOnCircularRoute()
    {
        // find NATS subject to redis subject matches
        for (Entry<String, String> subjEntry : subjectsToChannels.entrySet())
        {
            for (Entry<String, String> chanEntry : channelsToSubjects.entrySet())
            {
                if (subjEntry.getKey().equals(chanEntry.getValue()) &&
                        subjEntry.getValue().equals(chanEntry.getKey()))
                {
                    logger.warn("Possible circular route found between subject '{}' and channel '{}'",
                            subjEntry.getKey(), subjEntry.getValue());
                }
            }
        }
    }

    private void loadConfig() throws Exception
    {
       String configStr = defaultConfiguration;


        if (configUrl != null) {
            configStr = NATSUtilities.readFromUrl(configUrl);
        }

        JSONObject rootConfig = new JSONObject(new JSONTokener(configStr));

        host = rootConfig.optString("host", DEFAULT_REDIS_HOST);
        port = rootConfig.optInt("port", DEFAULT_REDIS_PORT);
        timeout = rootConfig.optInt("timeout", DEFAULT_REDIS_TIMEOUT);

        JSONArray ja = rootConfig.getJSONArray("nats_to_redis_map");
        if (ja != null) {
            for (int i = 0; i < ja.length(); i++) {
                parseNatsToRedisMapObj((JSONObject)ja.get(i));
            }
        }

        ja = rootConfig.getJSONArray("redis_to_nats_map");
        if (ja != null) {
            for (int i = 0; i < ja.length(); i++) {
                parseRedisToNatsMapObj((JSONObject)ja.get(i));
            }
        }

        warnOnCircularRoute();
    }

    private byte[] getChannelFromSubject(String subject)
    {
        String channel = subjectsToChannels.get(subject);
        if (channel == null)
            return null;

        return channel.getBytes();
    }

    private String getSubjectFromChannel(String channel)
    {
        return channelsToSubjects.get(channel);
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

            logger.trace("Published Message Redis ({}) -> NATS ({})", channelOrPattern, subject);
        }

        public void onMessage(String channel, String message)
        {
            sendNatsMessage(channel, message);
        }

        public void onSubscribe(String channel, int subscribedChannels)
        {
            logger.debug("Subscribed to Redis channel {} ({})", channel, subscribedChannels);
        }

        public void onUnsubscribe(String channel, int subscribedChannels) {
            logger.debug("Unsubscribed to Redis channel {} ({})", channel, subscribedChannels);
        }

        public void onPSubscribe(String pattern, int subscribedChannels) {
            logger.debug("Subscribed to Redis pattern {} ({})", pattern, subscribedChannels);
        }

        public void onPUnsubscribe(String pattern, int subscribedChannels) {
            logger.debug("Unsubscribed from Redis pattern  {} ({})", pattern, subscribedChannels);
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

        private String[] buildJedisSubscribeChannels()
        {
            if (channelsToSubjects == null)
                return null;

            if (channelsToSubjects.size() < 1)
                return null;

            return channelsToSubjects.keySet().toArray(new String[0]);
        }

        private void logChannels(String[] channels)
        {
            if (logger.isDebugEnabled() == false)
                return;

            if (channels == null || channels.length == 0) {
                logger.debug("Not subscribing to any redis channels.");
            }

            for (String chan : channels) {
                logger.debug("Subscribing to redis channel: {}", chan);
            }

        }

        @Override
        public void run()
        {
            listenJedis = jedisPool.getResource();

            while (isRunning())
            {
                // TODO - break this call when stopping...
                try {
                    String[] channels = buildJedisSubscribeChannels();
                    logChannels(channels);
                    if (channels == null)
                    {
                        shutdown();
                        break;
                    }
                    listenJedis.subscribe(new JedisListener(), channels);
                }
                catch (JedisConnectionException e)
                {
                    logger.error("Lost connection to the Redis server.  Exiting.");

                    // TODO:  retry logic?
                    shutdown();
                }
            }

            listenJedis.close();
        }

        public void shutdown()
        {
            setRunning(false);
            listenJedis.close();
        }
    }

    public RedisPubSubPlugin() {}

    private void initJedis()
    {
        jedisPool = new JedisPool(new JedisPoolConfig(), host, port, timeout);
        publishJedis = jedisPool.getResource();
    }

    private void teardownJedis()
    {
        if (listener != null)
            listener.shutdown();

        if (publishJedis != null)
            publishJedis.close();

        if (jedisPool != null)
            jedisPool.close();
    }

    @Override
    public boolean onStartup(Logger logger, ConnectionFactory factory) {
        this.logger = logger;

        try {
            loadProperties();
            loadConfig();
            initJedis();
        }
        catch (Exception e) {
            logger.error("Unable to initialize.", e);
            teardownJedis();
            return false;
        }

        return true;
    }

    @Override
    public boolean onNatsInitialized(NATSConnector connector)
    {
        this.connector = connector;

        if (subjectsToChannels == null && channelsToSubjects == null)
        {
            logger.error("No subject/channel mapping has been defined.");
            return false;
        }

        try {
            if (subjectsToChannels != null) {
                for (String s : subjectsToChannels.keySet()) {
                    connector.subscribe(s);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("NATS Subscription error", e);
            return false;
        }

        if (channelsToSubjects != null) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(new ListenForRedisUpdates());
        }

        return true;
    }

    @Override
    public void onShutdown()
    {
        teardownJedis();
    }

    @Override
    public void onNATSMessage(Message msg)
    {
        String subject = msg.getSubject();
        byte[] channel = getChannelFromSubject(subject);
        if (channel == null)
        {
            // TODO: Able to get here legitimately this with wildcard?
            logger.error("Cannot publish from NATS to Redis - unmapped subject '" + subject + "'");
            return;
        }

        publishJedis.publish(channel, msg.getData());

        logger.debug("Message NATS ({}) -> Redis", subject);
    }

    @Override
    public void onNATSEvent(NATSEvent event, String message)
    {
        // TODO:  Handle corner cases
        // Connection disconnected - close JEDIS, buffer messages?
        // Reconnected - reconnect to JEDIS.
        // Closed:  should handle elsewhere.
        // Async error.  Notify, let admins handle these.
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
                logger.warn("NATS Event (Unknown): " + message);
        }
    }
}
