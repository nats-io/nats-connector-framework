[![Stories in Ready](https://badge.waffle.io/nats-io/nats-connector.png?label=ready&title=Ready)](https://waffle.io/nats-io/nats-connector)
# NATS Connector 
A pluggable [Java](http://www.java.com) based service to bridge the [NATS messaging system](https://nats.io) and other technologies.

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/nats-io/nats-connector.svg?branch=master)](http://travis-ci.org/nats-io/nats-connector)
[![Javadoc](http://javadoc-badge.appspot.com/io.nats/nats-connector.svg?label=javadoc)](http://nats-io.github.io/nats-connector)
[![Coverage Status](https://coveralls.io/repos/nats-io/nats-connector/badge.svg?branch=master&service=github)](https://coveralls.io/github/nats-io/nats-connector?branch=master)


## Summary

The NATS connector is provided to facilitate the bridging of NATS and other technologies with an easy to use plug-in framework.  General application tasks and NATS connectivity are implemented, allowing a developer to focus on integration rather than application development tasks.  The java platform was chosen as to reach as many technologies as possible.

Some plug-ins will be provided and maintained by Apcera.

Documentation can be found [here](http://nats-io.github.io/nats-connector).

## Installation

### Maven Central

#### Releases

The NATS connector is currently alpha; there are no official maven releases at this time.

#### Snapshots

Snapshots are regularly uploaded to the Sonatype OSSRH (OSS Repository Hosting) using
the same Maven coordinates.
Add the following dependency to your project's `pom.xml`.

```xml
  <dependencies>
    ...
    <dependency>
      <groupId>io.nats</groupId>
      <artifactId>nats-connector</artifactId>
      <version>0.1.5-SNAPSHOT</version>
    </dependency>
  </dependencies>
```
If you don't already have your pom.xml configured for using Maven snapshots, you'll also need to add the following repository to your pom.xml.

```xml
<repositories>
    ...
    <repository>
        <id>sonatype-snapshots</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```
#### Building from source code (this repository)
First, download the source code:
```
git clone git@github.com:nats-io/nats-connector.git .
```

To build the library, use [maven](https://maven.apache.org/). From the root directory of the project:

```
mvn package verify
```
The jar file will be built in the `target` directory. Then copy the jar file to your classpath and you're all set.

NOTE: running the unit tests requires that `gnatsd` be installed on your system and available in your executable search path.  For the NATS redis plugin a redis server must be installed and running locally at the default port.

### Source code (this repository)
To download the source code:
```
git clone git@github.com:nats-io/nats-connector.git .
```

To build the library, use [maven](https://maven.apache.org/). From the root directory of the project:

```
mvn verify package
```

## NATS connector source package structure

* io.nats.connector - Connector application and data flow management
* io.nats.connector.plugin - Interfaces, Classes, and Enums used by plugin developers.
* io.nats.connector.plugins - Out of the box plugins, developed by Apcera.

## Configuration
NATS configuration is set through the jnats client library properties and can be passed into the jvm, or specified in a configuration file.  The properties are described [here](http://nats-io.github.io/jnats/io/nats/client/Constants.html).

There is only one NATS connector property, which specifies the plugin class used.

com.io.nats.connector.plugin=classname of the plugin

For example, to use the NATS Redis plugin, specify:
```
com.io.nats.connector.plugin=io.nats.connector.plugins.redis.RedisPubSubPlugin
```

## Running the connector

There are two ways to launch the connector - invoking the connector as an application or programatically from your own application.

To invoke the connector from an application:
```
System.setProperty(Connector.PLUGIN_CLASS, <plugin class name>);
new Connector().run();
```
or as an application:
```
java -Dio.nats.connector.plugin=<plugin class name> io.nats.connector.Connector
```

If not using maven, ensure your classpath includes the most current nats-connector and jnats archives, as well as java archives compatible with jedis-2.7.3.jar, commons-pool2-2.4.2.jar, slf4j-simple-1.7.14.jar, slf4j-api-1.7.14.jar, jnats-0.3.1.jar, and json-20151123.jar.

## Apcera Supported Plugins

* [Redis Publish/Subscribe](https://github.com/nats-io/nats-connector-redis-plugin)

## Plugin Development

Plugin development is very straightforward, simply reference the plugin with maven coordinates above, implement the NATSConnectorPlugin interface, then when launching the NATS connector, reference your plugin with the Connector.PLUGIN_CLASS property.


```java
/**
 * This interface that must be implemented for a NATS Connector plugin.
 *
 * The order of calls are:
 *
 * onStartup
 * onNatsIntialized
 *
 * ...
 * onNatsMessage, onNATSEvent
 * ...
 * onShutdown
 */
public interface NATSConnectorPlugin {

    /**
     * Invoked when the connector is started up, before a connection
     * to the NATS cluster is made.  The NATS connection factory is
     * valid at this time, providing an opportunity to alter
     * NATS connection parameters based on other plugin variables.
     *
     * @param logger - logger for the NATS connector process.
     * @param factory - the NATS connection factory.
     * @return - true if the connector should continue, false otherwise.
     */
    public boolean onStartup(Logger logger, ConnectionFactory factory);

    /**
     * Invoked after startup, when the NATS plug-in has connectivity to the
     * NATS cluster, and is ready to start sending and
     * and receiving messages.  This is the place to create NATS subscriptions.
     *
     * @param connector interface to the NATS connector
     *
     * @return true if the plugin can continue.
     */
    public boolean onNatsInitialized(NATSConnector connector);

    /**
     * Invoked anytime a NATS message is received to be processed.
     * @param msg - NATS message received.
     */
    public void onNATSMessage(io.nats.client.Message msg);

    /**
     * Invoked anytime a NATS event occurs around a connection
     * or error, alerting the plugin to take appropriate action.
     *
     * For example, when NATS is reconnecting, buffer or pause incoming
     * data to be sent to NATS.
     *
     * @param event the type of event
     * @param message a string describing the event
     */
    public void onNATSEvent(NATSEvent event, String message);


    /**
     * Invoked when the Plugin is shutting down.  This is typically where
     * plugin resources are cleaned up.
     */
    public void onShutdown();
}
```

Plugins will require certain level NATS functionality.  For convenience, a NATS Connector object is passed to the plugin after  NATS has been initialized, making it simple to subscribe and publish messages.  If additional NATS functionality is required beyond what is provided, the Connection and Connection factory can be obtained for advanced usage.
```java
public interface NATSConnector {

    /**
     * In case of a critical failure or security issue, this allows the plugin
     * to request a shutdown of the connector.
     */
    public void shutdown();

    /***
     * Publishes a message into the NATS cluster.
     *
     * @param message - the message to publish.
     */
    public void publish(io.nats.client.Message message);

    /***
     * Flushes any pending NATS data.
     *
     * @throws  Exception - an error occurred in the flush.
     */
    public void flush() throws Exception;

    /***
     * Adds interest in a NATS subject.
     * @param subject - subject of interest.
     * @throws Exception - an error occurred in the subsciption process.
     */
    public void subscribe(String subject) throws Exception;

    /***
     * Adds interest in a NATS subject, with a custom handle.
     * @param subject - subject of interest.
     * @param handler - message handler
     * @throws Exception - an error occurred in the subsciption process.
     */
    public void subscribe(String subject, MessageHandler handler) throws Exception;

    /***
     * Adds interest in a NATS subject with a queue group.
     * @param subject - subject of interest.
     * @param queue - work queue
     * @throws Exception - an error occurred in the subsciption process.
     */
    public void subscribe(String subject, String queue) throws Exception;

    /***
     * Adds interest in a NATS subject with a queue group, with a custom handler.
     * @param subject - subject of interest.
     * @param queue - work queue
     * @param handler - message handler
     * @throws Exception - an error occurred in the subsciption process.
     */
    public void subscribe(String subject, String queue, MessageHandler handler) throws Exception;

    /***
     * Removes interest in a NATS subject
     * @param subject - subject of interest.
     */
    public void unsubscribe(String subject);

    /***
     * Advanced API to get the NATS connection.  This allows for NATS functionality beyond
     * the interface here.
     *
     * @return The connection to the NATS cluster.
     */
    public Connection getConnection();

    /***
     * Advanced API to get the Connection Factory, This allows for NATS functionality beyond
     * the interface here.
     *
     * @return The NATS connector ConnectionFactory
     */
    public ConnectionFactory getConnectionFactory();
}
```


## TODO

### Connector
- [X] Travis CI
- [ ] Differentiate input and output plugins.
- [ ] Remove dependency chain on Apcera supported plugins.
- [ ] RabbitMQ plugin
- [ ] Kafka Plugin
- [ ] Create containers
- [X] Maven Central

## Other potential plugins 
* [Kafka](http://kafka.apache.org/documentation.html)
* [HDFS](https://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/fs/FileSystem.html)
* [RabbitMQ](https://www.rabbitmq.com/api-guide.html)
* [ElastiSearch](https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/client.html)
* [JMS](http://docs.oracle.com/javaee/6/api/javax/jms/package-summary.html)
* File Transfer - A basic file xfer utility
* [IBM MQ](http://www-01.ibm.com/support/knowledgecenter/SSFKSJ_7.5.0/com.ibm.mq.dev.doc/q030520_.htm)

Suggestions and implementations are always welcome!
