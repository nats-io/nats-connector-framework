package io.nats.connector;

import io.nats.client.Message;
import io.nats.connector.plugin.NATSConnector;
import io.nats.connector.plugin.NATSConnectorPlugin;
import io.nats.connector.plugin.NATSEvent;
import org.slf4j.Logger;
import sun.jvm.hotspot.utilities.Assert;

import org.junit.Test;

/**
 * Created by colinsullivan on 12/30/15.
 */
public class DataFlowHandlerTest {


    private class TestPlugin implements NATSConnectorPlugin {

        public static final String pluginName = "testplugin";

        boolean getNameCalled;
        boolean onStartupCalled;
        boolean onShutdownCalled;
        boolean onNatsInitializedCalled;
        boolean onNatsMessageCalled;
        boolean onNatsEventCalled;

        boolean didAllCallsSucceed()
        {
            return (getNameCalled &&
                    onStartupCalled &&
                    onShutdownCalled &&
                    onNatsInitializedCalled &&
                    onNatsMessageCalled &&
                    onNatsEventCalled);
        }

        @Override
        public String getName() {
            return pluginName;
        }

        @Override
        public boolean OnStartup(Logger logger) {
            Assert.that(logger != null, "logger is null");
            return true;
        }

        @Override
        public void OnShutdown() {

        }

        @Override
        public boolean OnNatsInitialized(NATSConnector connector) {
            return false;
        }

        @Override
        public void OnNATSMessage(Message msg) {

        }

        @Override
        public void OnNATSEvent(NATSEvent event, String message) {

        }
    }

    @Test
    public void testBasicPlugin()
    {
        TestPlugin plugin = new TestPlugin();
        //DataFlowHandler df = new DataFlowHandler(plugin, );
    }

}