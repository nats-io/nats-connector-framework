// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for the NATS connector framework.
 */
public class ConnectorTest {


    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        UnitTestUtilities.startDefaultServer();
        Thread.sleep(500);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        UnitTestUtilities.stopDefaultServer();
        Thread.sleep(500);
    }

    @Test
    public void testRun() throws Exception {

        try {
            System.setProperty(Connector.PLUGIN_CLASS, this.getClass().getCanonicalName());
            new Connector().run();
        }
        catch (Exception e)
        {
            // unable to cast.
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        try {
            // unable to find.
            System.setProperty(Connector.PLUGIN_CLASS, "missing.gone.nothere.awol");
            new Connector().run();
        }
        catch (Exception e)
        {
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        try {
            // The plugin throws an exception.
            System.setProperty(Connector.PLUGIN_CLASS, ExceptionTestPlugin.class.getName());
            new Connector().run();
        }
        catch (Exception e)
        {
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        System.setProperty(Connector.PLUGIN_CLASS, NatsTestPlugin.class.getName());
        new Connector().run();
    }
}