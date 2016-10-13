// Copyright 2015 Apcera Inc. All Rights Reserved.

package io.nats.connector;

import io.nats.connector.plugin.NATSUtilities;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test for the NATS connector framework.
 */
@Category(UnitTest.class)
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
    public void testStartErrors() throws Exception {

        try {
            // No plugin class specified
            System.clearProperty(Connector.PLUGIN_CLASS);
            new Connector().run();
        } catch (Exception e) {
            // unable to start.
            System.out.println("Received Expected Exception: " + e.getMessage());
        }
        try {
            System.setProperty(Connector.PLUGIN_CLASS, this.getClass().getCanonicalName());
            new Connector().run();
        } catch (Exception e) {
            // unable to cast.
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        try {
            // unable to find.
            System.setProperty(Connector.PLUGIN_CLASS, "missing.gone.nothere.awol");
            new Connector().run();
        } catch (ClassNotFoundException e) {
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        try {
            // security exception
            System.setProperty(Connector.PLUGIN_CLASS, "java.lang.Runtime");
            new Connector().run();
        } catch (IllegalAccessException e) {
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        try {
            // security exception
            System.setProperty(Connector.PLUGIN_CLASS, "java.lang.Number");
            new Connector().run();
        } catch (InstantiationException e) {
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        try {
            // The plugin throws an exception.
            System.setProperty(Connector.PLUGIN_CLASS, ExceptionTestPlugin.class.getName());
            new Connector().run();
        } catch (Exception e) {
            System.out.println("Received Expected Exception: " + e.getMessage());
        }

        System.setProperty(Connector.PLUGIN_CLASS,
                io.nats.connector.plugins.test.NATSTestPlugin.class.getName());
        new Connector().run();
    }

    @Test
    public void testConnectorReadFromURL() throws Exception {
        NATSUtilities.readFromUrl("http://google.com");
        try {
            NATSUtilities.readFromUrl("garbage");
            Assert.fail("Did not receive expected message");
        } catch (Exception e) {
            ;;
        }
    }

    @Test
    public void testConnectorStart() throws Exception {
        System.setProperty(Connector.PLUGIN_CLASS,
                io.nats.connector.plugins.test.NATSTestPlugin.class.getName());
        new Connector().run();
    }

    @Test
    public void testConnectorStartFromMain() throws Exception {

        System.setProperty(Connector.PLUGIN_CLASS,
                io.nats.connector.plugins.test.NATSTestPlugin.class.getName());

        try {
            Connector.main(new String[0]);
        } catch (Exception ex) {
            Assert.fail("Expected success.");
        }

        // invalid configuration, just make sure we don't crash
        String[] s = { "-config", "lksdfj" };
        Connector.main(s);

        PrintWriter pw = new PrintWriter("config.props");
        pw.write(Connector.PLUGIN_CLASS + "="
                + io.nats.connector.plugins.test.NATSTestPlugin.class.getName());
        pw.close();

        System.clearProperty(Connector.PLUGIN_CLASS);
        // make sure we don't crash, and cover code.
        s[1] = "config.props";
        Connector.main(s);

    }

    @Test
    public void testConnectorShutdown() throws Exception {
        System.setProperty(Connector.PLUGIN_CLASS,
                io.nats.connector.plugins.test.NATSTestPlugin.class.getName());
        final Connector c = new Connector();
        c.shutdown();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                c.run();
            }
        });
        Thread.sleep(1000);
        c.shutdown();
    }

    @Test
    public void testDisconnectReconnect() throws Exception {

        System.setProperty(Connector.PLUGIN_CLASS,
                io.nats.connector.plugins.test.NATSTestPlugin.class.getName());
        Connector c = new Connector();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(500);
                    System.out.println("Stopping NATS server.");
                    UnitTestUtilities.stopDefaultServer();

                    Thread.sleep(1000);

                    System.out.println("Starting NATS server.");
                    UnitTestUtilities.startDefaultServer();
                } catch (InterruptedException e) {
                    ;;
                }
            }
        });

        c.run();
    }

    @Test
    public void testNoNATSServer() throws Exception {

        UnitTestUtilities.stopDefaultServer();

        System.setProperty(Connector.PLUGIN_CLASS,
                io.nats.connector.plugins.test.NATSTestPlugin.class.getName());
        new Connector().run();

        UnitTestUtilities.stopDefaultServer();
    }
}
