package io.nats.connector.plugins;

import io.nats.connector.Connector;
import io.nats.connector.ExceptionTestPlugin;
import io.nats.connector.NatsTestPlugin;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by colinsullivan on 12/31/15.
 */
public class RedisConnectorPluginTest {
    @Test
    public void testRun() throws Exception {

        System.setProperty(Connector.USER_PROP_PLUGIN_CLASS, RedisConnectorPlugin.class.getName());
        System.setProperty("org.slf4j.simpleLogger.log.io.nats.connector.plugins.RedisConnectorPlugin", "debug");
        new Connector().run();
    }
}