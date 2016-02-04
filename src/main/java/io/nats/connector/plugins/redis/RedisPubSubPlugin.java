/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/

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

import java.util.ArrayList;
import java.util.Properties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.List;

/**
 * A Redis publish/subscribe connector plugin.
 * <p>
 * It reads a configuration file from a provided url to direct
 * the connector to bridge NATS and Redis.
 * <p>
 * The file is JSON formatted with the following structure:
 * <pre>
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
 * </pre>
 * <p>
 * NATs publishing to Redis.Export will send messages to Redis on channel
 * NATS.import.
 * <p>
 * Redis publishing to NATS.Export will send messages to NATS on subject
 * Redis.Import.
 * <p>
 * This is highly customizable by adding multiple subscriptions.
 * <p>
 * Wildcards/Patters are not yet supported.
 * <p>
 * Take care to avoid circular routes generated
 * by overlapping maps should be avoided.
 */
public class RedisPubSubPlugin implements NATSConnectorPlugin  {

    /**
     * The property location to specify a configuration URL.
     */
    static public final String CONFIG_URL = "nats.io.connector.plugins.redispubsub.configurl";

    /**
     * Default redis host.
     */
    static public final String DEFAULT_REDIS_HOST = "localhost";

    /**
     * Default redis port.
     */
    static public final int DEFAULT_REDIS_PORT = 6379;

    /**
     * Default redis timeout.
     */
    static public final int DEFAULT_REDIS_TIMEOUT = 2000;

    NATSConnector connector = null;
    Logger logger = null;

    boolean trace = false;

    private HashMap<String, List<String>> channelsToSubjects = null;
    private HashMap<String, List<String>> subjectsToChannels = null;

    JedisPool    jedisPool        = null;
    BinaryJedis  publishJedis     = null;

    Object       redisPublishLock = new Object();

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

    private boolean isNatsSubjectValid(String subject)
    {
        if (subject == null)
        {
            logger.warn("Null NATS subjects are not valid.");
            return false;
        }

        if (subject.isEmpty())
        {
            logger.warn("Empty NATS subject is are not valid.");
            return false;
        }

        if (subject.contains(">") || subject.contains("*"))
        {
            logger.warn("Wildcard NATS subject {} is not supported.", subject);
            return false;
        }

        return true;
    }

    private boolean listContains(List<String> list, String value)
    {
        for (String s : list)
        {
            if (s.equals(value))
            {
                // just silently ignore duplicate mappings.
                return true;
            }
        }

        return false;
    }

    private void addToMap(HashMap<String, List<String>> map, String key, String value)
    {
        List <String> l = null;

        if (map == null)
            return;

        if (map.containsKey(key) == false)
        {
            l = new ArrayList<String>();
            l.add(value);
            map.put(key, l);
        }
        else
        {
            l = map.get(key);

            // just silenly ignore duplicates.
            if (listContains(l, value))
                return;

            l.add(value);
        }
    }

    private void parseRedisToNatsMapObj(JSONObject destMapObj) throws Exception
    {
        checkMapObj(destMapObj);

        if (channelsToSubjects == null)
            channelsToSubjects = new HashMap<String, List<String>>();

        String channel = destMapObj.getString("channel");
        String subject = destMapObj.getString("subject");

        if (!isNatsSubjectValid(subject))
            throw new Exception("Invalid subject: " + subject);

        logger.debug("Mapping Redis channel {} to NATS subject {}",
                channel, subject);

        addToMap(channelsToSubjects, channel, subject);
    }

    private void parseNatsToRedisMapObj(JSONObject destMapObj) throws Exception
    {
        checkMapObj(destMapObj);

        if (subjectsToChannels == null)
            subjectsToChannels = new HashMap<String, List<String>>();

        String channel = destMapObj.getString("channel");
        String subject = destMapObj.getString("subject");

        if (!isNatsSubjectValid(subject))
            throw new Exception("Invalid subject: " + subject);

        logger.debug("Mapping NATS subject {} to Redis channel {}",
                subject, channel);

        addToMap(subjectsToChannels, subject, channel);
    }

    // Warn against circular routes; they don't end well.
    //
    // TODO:  Wildcard/Pattern checks someday.
    private void warnOnCircularRoute()
    {
        if (subjectsToChannels == null || channelsToSubjects == null)
            return;

        // find NATS subject to redis subject matches
        for (Entry<String, List<String>> subjEntry : subjectsToChannels.entrySet())
        {
            for (Entry<String, List<String>> chanEntry : channelsToSubjects.entrySet())
            {
                if (listContains(chanEntry.getValue(), subjEntry.getKey()) &&
                        listContains(subjEntry.getValue(), (chanEntry.getKey())))
                {
                    logger.error("Circular route found between subject '{}' and channel '{}'",
                            subjEntry.getKey(), subjEntry.getValue());
                }
            }
        }
    }


    /**
     * Gets the default configuration.
     * @return default configuration as a JSON string.
     */
    String getDefaultConfiguration()
    {
        return defaultConfiguration;
    }

    private void loadConfig() throws Exception {

        String configStr = getDefaultConfiguration();

        if (configUrl != null) {
            configStr = NATSUtilities.readFromUrl(configUrl);
        }

        parseConfiguration(configStr);
    }

    /**
     * Public for testing purposes.
     *
     * @param jsonConfig - json configuration in a string.
     * @throws Exception - an error occurred parsing the configuration.
     */
    public void parseConfiguration(String jsonConfig) throws Exception
    {
        JSONArray ja;

        JSONObject rootConfig = new JSONObject(new JSONTokener(jsonConfig));

        host = rootConfig.optString("host", DEFAULT_REDIS_HOST);
        port = rootConfig.optInt("port", DEFAULT_REDIS_PORT);
        timeout = rootConfig.optInt("timeout", DEFAULT_REDIS_TIMEOUT);

        if (rootConfig.has("nats_to_redis_map")) {
            ja = rootConfig.getJSONArray("nats_to_redis_map");
            if (ja != null) {
                for (int i = 0; i < ja.length(); i++) {
                    parseNatsToRedisMapObj((JSONObject) ja.get(i));
                }
            }
        }

        if (rootConfig.has("redis_to_nats_map")) {
            ja = rootConfig.getJSONArray("redis_to_nats_map");
            if (ja != null) {
                for (int i = 0; i < ja.length(); i++) {
                    parseRedisToNatsMapObj((JSONObject) ja.get(i));
                }
            }
        }

        warnOnCircularRoute();
    }

    private List<String> getChannelsFromSubject(String subject)
    {
        if (subjectsToChannels == null)
            return null;

        return subjectsToChannels.get(subject);
    }

    private List<String> getSubjectsFromChannel(String channel)
    {
        if (channelsToSubjects == null)
            return null;

        return channelsToSubjects.get(channel);
    }

    private class JedisListener extends JedisPubSub
    {
        List<String> l;

        Message natsMessage = new Message();

        private void sendNatsMessage(String channelOrPattern, String message)
        {
            l = getSubjectsFromChannel(channelOrPattern);

            byte[] payload = message.getBytes();
            natsMessage.setData(payload, 0, payload.length);

            for (String s : l) {
                natsMessage.setSubject(s);
                connector.publish(natsMessage);

                logger.trace("Send Redis ({}) -> NATS ({})", channelOrPattern, s);
            }
        }

        @Override
        public void onMessage(String channel, String message)
        {
            sendNatsMessage(channel, message);
            super.onMessage(channel, message);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels)
        {
            logger.debug("Subscribed to Redis channel {} ({})", channel, subscribedChannels);

            super.onSubscribe(channel, subscribedChannels);
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels)
        {
            logger.debug("Unsubscribed to Redis channel {} ({})", channel, subscribedChannels);

            super.onUnsubscribe(channel, subscribedChannels);
        }

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels)
        {
            logger.debug("Subscribed to Redis pattern {} ({})", pattern, subscribedChannels);

            super.onPSubscribe(pattern, subscribedChannels);
        }

        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels)
        {
            logger.debug("Unsubscribed from Redis pattern  {} ({})", pattern, subscribedChannels);

            super.onPUnsubscribe(pattern, subscribedChannels);
        }

        @Override
        public void onPMessage(String pattern, String channel,
                               String message)
        {
            sendNatsMessage(channel, message);

            super.onPMessage(pattern, channel, message);
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
        logger.debug("Initializing Redis.");

        jedisPool = new JedisPool(new JedisPoolConfig(), host, port, timeout);
        publishJedis = jedisPool.getResource();
    }

    private void teardownJedis()
    {
        logger.debug("Cleaning up Redis Resources.");

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
        List<String> channels = getChannelsFromSubject(subject);
        if (channels == null)
        {
            logger.error("Cannot publish from NATS to Redis - unmapped subject '" + subject + "'");
            return;
        }

        byte[] payload = msg.getData();

        for (String s : channels)
        {

            byte[] channel = s.getBytes();

            // NOTE:  in tests, one can get an EOS exception here when shutting down.
            //
            // Also, a jedis instance is not threadsafe.  Right now simply lock.
            // If we need better performance, investigate using one per
            // NATS subject.
            synchronized (redisPublishLock) {
                publishJedis.publish(channel, payload);
            }

            logger.trace("Send NATS ({}) -> Redis ({})", subject, new String(channel));
        }
    }

    @Override
    public void onNATSEvent(NATSEvent event, String message)
    {
        // When a connection has been disconnected unexpectedly, NATS will
        // try to reconnect.  Messages published during the reconnect will
        // be buffered and resent, so there may be no need to do anything.
        // Connection disconnected - close JEDIS, buffer messages?
        // Reconnected - reconnect to JEDIS.
        // Closed:  should handle elsewhere.
        // Async error.  Notify, let admins handle these.
        switch (event)
        {
            case ASYNC_ERROR:
                logger.error("NATS Asynchronous error: " + message);
                break;
            case RECONNECTED:
                logger.info("Reconnected to the NATS cluster: " + message);
                // At this point, we may not have to do much.  Buffered NATS messages
                // may be flushed. and we'll buffer and flush the Redis messages.
                // Revisit this later if we need more buffering.
                break;
            case DISCONNECTED:
                logger.info("Disconnected from the NATS cluster: " + message);
                break;
            case CLOSED:
                logger.debug("NATS Event Connection Closed: " + message);
                // shudown - if this is a result of shutdown elsewhere,
                // there will be no effect.
                connector.shutdown();
                break;
            default:
                logger.warn("Unknown NATS Event: " + message);
        }
    }
}
