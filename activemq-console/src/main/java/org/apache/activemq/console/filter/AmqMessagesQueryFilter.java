/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.console.filter;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

public class AmqMessagesQueryFilter extends AbstractQueryFilter {

    private URI brokerUrl;
    private Destination destination;

    private ConnectionFactory connectionFactory;

    /**
     * Create a JMS message query filter
     *
     * @param brokerUrl   - broker url to connect to
     * @param destination - JMS destination to query
     */
    public AmqMessagesQueryFilter(URI brokerUrl, Destination destination) {
        super(null);
        this.brokerUrl = brokerUrl;
        this.destination = destination;
    }

    /**
     * Create a JMS message query filter
     *
     * @param connectionFactory - to connect with
     * @param destination - JMS destination to query
     */
    public AmqMessagesQueryFilter(ConnectionFactory connectionFactory, Destination destination) {
        super(null);
        this.destination = destination;
        this.connectionFactory = connectionFactory;
    }

    /**
     * Queries the specified destination using the message selector format query
     *
     * @param queries - message selector queries
     * @return list messages that matches the selector
     * @throws Exception
     */
    public List query(List queries) throws Exception {
        String selector = "";

        // Convert to message selector
        for (Object query : queries) {
            selector = selector + "(" + query.toString() + ") AND ";
        }

        // Remove last AND
        if (!selector.equals("")) {
            selector = selector.substring(0, selector.length() - 5);
        }

        if (destination instanceof ActiveMQQueue) {
            return queryMessages((ActiveMQQueue) destination, selector);
        } else {
            return queryMessages((ActiveMQTopic) destination, selector);
        }
    }

    /**
     * Query the messages of a queue destination using a queue browser
     *
     * @param queue    - queue destination
     * @param selector - message selector
     * @return list of messages that matches the selector
     * @throws Exception
     */
    protected List queryMessages(ActiveMQQueue queue, String selector) throws Exception {
        Connection conn = createConnection();

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueBrowser browser = sess.createBrowser(queue, selector);

        List messages = Collections.list(browser.getEnumeration());

        conn.close();

        return messages;
    }

    /**
     * Query the messages of a topic destination using a message consumer
     *
     * @param topic    - topic destination
     * @param selector - message selector
     * @return list of messages that matches the selector
     * @throws Exception
     */
    protected List queryMessages(ActiveMQTopic topic, String selector) throws Exception {
        // TODO: should we use a durable subscriber or a retroactive non-durable
        // subscriber?
        // TODO: if a durable subscriber is used, how do we manage it?
        // subscribe/unsubscribe tasks?
        return null;
    }

    /**
     * Create and start a JMS connection
     *
     * @return JMS connection
     * @throws JMSException
     */
    protected Connection createConnection() throws JMSException {
        // maintain old behaviour, when called either way.
        if (null == connectionFactory) {
            connectionFactory = (new ActiveMQConnectionFactory(getBrokerUrl()));
        }
        Connection conn = connectionFactory.createConnection();
        conn.start();
        return conn;
    }

    /**
     * Get the broker url being used.
     *
     * @return broker url
     */
    public URI getBrokerUrl() {
        return brokerUrl;
    }

    /**
     * Set the broker url to use.
     *
     * @param brokerUrl - broker url
     */
    public void setBrokerUrl(URI brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    /**
     * Get the destination being used.
     *
     * @return - JMS destination
     */
    public Destination getDestination() {
        return destination;
    }

    /**
     * Set the destination to use.
     *
     * @param destination - JMS destination
     */
    public void setDestination(Destination destination) {
        this.destination = destination;
    }
}
