package io.nats.connector;

import org.junit.Test;

/**
 * Created by colinsullivan on 12/30/15.
 */
public class ConnectorTest {

    @Test
    public void testRun() throws Exception {

        try {
            System.setProperty(Connector.USER_PROP_PLUGIN_CLASS, this.getClass().getCanonicalName());
            new Connector().run();
        }
        catch (Exception e)
        {
            // unable to cast.
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        try {
            // unable to find.
            System.setProperty(Connector.USER_PROP_PLUGIN_CLASS, "missing.gone.nothere.awol");
            new Connector().run();
        }
        catch (Exception e)
        {
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        System.setProperty(Connector.USER_PROP_PLUGIN_CLASS, NatsTestPlugin.class.getName());
        new Connector().run();

        System.setProperty(Connector.USER_PROP_PLUGIN_CLASS, ExceptionTestPlugin.class.getName());
        new Connector().run();
    }
}