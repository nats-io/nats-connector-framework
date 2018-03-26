// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.connector.plugin;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.MessageHandler;

/**
 * Interface for the utility class passed to the plug-in, allowing additional
 * NATS functionality such as publishing messsages and subscribing to
 * subjects.
 */
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
