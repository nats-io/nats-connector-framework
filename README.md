# nats-connector
A pluggable service to bridge NATS with other technologies

## Work in Progress
Code here is prototype/experimental code, incomplete, and not ready for public consumption.  Feel free to browse, but using this right now would not be a good idea.

### Source code (this repository)
To download the source code:
```
git clone git@github.com:nats-io/nats-connector.git .
```

To build the library, use [maven](https://maven.apache.org/). From the root directory of the project:

```
mvn clean package -DskipTests
```


## NATS connector source structure

* io.nats.connector - Connector application and data flow management
* io.nats.connector.plugin - Interfaces, Classes, and Enums used by plugins.  Plugin developers use this to build their plugins.
* io.nats.connector.plugins - Out of the box plugins, developed by Apcera.

## Configuration
On the NATS side, it is very simple, simply set java preferences NATS will use.  (See jnats)

There is only one native NATS connector parameter:
com.io.nats.connector.plugin=classname of the plugin

All other NATS related parameters are set via java properties, passed through as a property file
or JVM parameters.

## Plugins

### Redis plugin

The redis plugin is:
```
com.io.nats.connector.plugins.redis.RedisPubSubPlugin
```
