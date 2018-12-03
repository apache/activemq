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
package org.apache.activemq.transport.amqp;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.junit.After;

public class JMSClientTestSupport extends AmqpTestSupport {

    protected Connection connection;

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            LOG.debug("tearDown started.");
            if (connection != null) {
                LOG.debug("in CloseConnectionTask.call(), calling connection.close()");
                connection.close();
            }
        } finally {
            connection = null;
            super.tearDown();
        }
    }

    /**
     * @return the proper destination name to use for each test method invocation.
     */
    protected String getDestinationName() {
        return name.getMethodName();
    }

    /**
     * Can be overridden in subclasses to test against a different transport suchs as NIO.
     *
     * @return the URI to connect to on the Broker for AMQP.
     */
    protected URI getBrokerURI() {
        return amqpURI;
    }

    protected URI getAmqpURI() {
        return getAmqpURI("");
    }

    protected URI getAmqpURI(String uriOptions) {

        String clientScheme;
        boolean useSSL = false;

        switch (getBrokerURI().getScheme()) {
            case "tcp" :
            case "amqp":
            case "auto":
            case "amqp+nio":
            case "auto+nio":
                clientScheme = "amqp://";
                break;
            case "ssl":
            case "amqp+ssl":
            case "auto+ssl":
            case "amqp+nio+ssl":
            case "auto+nio+ssl":
                clientScheme = "amqps://";
                useSSL = true;
                break;
            case "ws":
            case "amqp+ws":
                clientScheme = "amqpws://";
                break;
            case "wss":
            case "amqp+wss":
                clientScheme = "amqpwss://";
                useSSL = true;
                break;
            default:
                clientScheme = "amqp://";
        }

        String amqpURI = clientScheme + getBrokerURI().getHost() + ":" + getBrokerURI().getPort();

        if (uriOptions != null && !uriOptions.isEmpty()) {
            if (uriOptions.startsWith("?") || uriOptions.startsWith("&")) {
                uriOptions = uriOptions.substring(1);
            }
        } else {
            uriOptions = "";
        }

        if (useSSL) {
            amqpURI += "?transport.verifyHost=false";
        }

        if (!uriOptions.isEmpty()) {
            if (useSSL) {
                amqpURI += "&" + uriOptions;
            } else {
                amqpURI += "?" + uriOptions;
            }
        }

        URI result = getBrokerURI();
        try {
            result = new URI(amqpURI);
        } catch (URISyntaxException e) {
        }

        return result;
    }

    protected Connection createConnection() throws JMSException {
        return createConnection(name.toString(), false);
    }

    protected Connection createConnection(boolean syncPublish) throws JMSException {
        return createConnection(name.toString(), syncPublish);
    }

    protected Connection createConnection(String clientId) throws JMSException {
        return createConnection(clientId, false);
    }

    protected Connection createConnection(String clientId, boolean syncPublish) throws JMSException {
        Connection connection = JMSClientContext.INSTANCE.createConnection(getBrokerURI(), "admin", "password", clientId, syncPublish);
        connection.start();
        return connection;
    }
}
