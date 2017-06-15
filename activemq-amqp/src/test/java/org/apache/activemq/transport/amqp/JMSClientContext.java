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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context used for AMQP JMS Clients to create connection instances.
 */
public class JMSClientContext {

    private static final Logger LOG = LoggerFactory.getLogger(JMSClientContext.class);

    public static final JMSClientContext INSTANCE = new JMSClientContext();

    //----- Plain JMS Connection Create methods ------------------------------//

    public Connection createConnection(URI remoteURI) throws JMSException {
        return createConnection(remoteURI, null, null, true);
    }

    public Connection createConnection(URI remoteURI, String username, String password) throws JMSException {
        return createConnection(remoteURI, username, password, null, true);
    }

    public Connection createConnection(URI remoteURI, String username, String password, boolean syncPublish) throws JMSException {
        return createConnection(remoteURI, username, password, null, syncPublish);
    }

    public Connection createConnection(URI remoteURI, String username, String password, String clientId) throws JMSException {
        return createConnection(remoteURI, username, password, clientId, true);
    }

    public Connection createConnection(URI remoteURI, String username, String password, String clientId, boolean syncPublish) throws JMSException {
        ConnectionFactory factory = createConnectionFactory(remoteURI, username, password, syncPublish);

        Connection connection = factory.createConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                LOG.error("Unexpected exception ", exception);
                exception.printStackTrace();
            }
        });

        if (clientId != null && !clientId.isEmpty()) {
            connection.setClientID(clientId);
        }

        return connection;
    }

    //----- JMS TopicConnection Create methods -------------------------------//

    public TopicConnection createTopicConnection(URI remoteURI) throws JMSException {
        return createTopicConnection(remoteURI, null, null, true);
    }

    public TopicConnection createTopicConnection(URI remoteURI, String username, String password) throws JMSException {
        return createTopicConnection(remoteURI, username, password, null, true);
    }

    public TopicConnection createTopicConnection(URI remoteURI, String username, String password, boolean syncPublish) throws JMSException {
        return createTopicConnection(remoteURI, username, password, null, syncPublish);
    }

    public TopicConnection createTopicConnection(URI remoteURI, String username, String password, String clientId) throws JMSException {
        return createTopicConnection(remoteURI, username, password, clientId, true);
    }

    public TopicConnection createTopicConnection(URI remoteURI, String username, String password, String clientId, boolean syncPublish) throws JMSException {
        TopicConnectionFactory factory = createTopicConnectionFactory(remoteURI, username, password, syncPublish);

        TopicConnection connection = factory.createTopicConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                LOG.error("Unexpected exception ", exception);
                exception.printStackTrace();
            }
        });

        if (clientId != null && !clientId.isEmpty()) {
            connection.setClientID(clientId);
        }

        return connection;
    }

    //----- JMS QueueConnection Create methods -------------------------------//

    public QueueConnection createQueueConnection(URI remoteURI) throws JMSException {
        return createQueueConnection(remoteURI, null, null, true);
    }

    public QueueConnection createQueueConnection(URI remoteURI, String username, String password) throws JMSException {
        return createQueueConnection(remoteURI, username, password, null, true);
    }

    public QueueConnection createQueueConnection(URI remoteURI, String username, String password, boolean syncPublish) throws JMSException {
        return createQueueConnection(remoteURI, username, password, null, syncPublish);
    }

    public QueueConnection createQueueConnection(URI remoteURI, String username, String password, String clientId) throws JMSException {
        return createQueueConnection(remoteURI, username, password, clientId, true);
    }

    public QueueConnection createQueueConnection(URI remoteURI, String username, String password, String clientId, boolean syncPublish) throws JMSException {
        QueueConnectionFactory factory = createQueueConnectionFactory(remoteURI, username, password, syncPublish);

        QueueConnection connection = factory.createQueueConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                LOG.error("Unexpected exception ", exception);
                exception.printStackTrace();
            }
        });

        if (clientId != null && !clientId.isEmpty()) {
            connection.setClientID(clientId);
        }

        return connection;
    }

    //------ Internal Implementation bits ------------------------------------//

    private QueueConnectionFactory createQueueConnectionFactory(
        URI remoteURI, String username, String password, boolean syncPublish) {

        return (QueueConnectionFactory) createConnectionFactory(remoteURI, username, password, syncPublish);
    }

    private TopicConnectionFactory createTopicConnectionFactory(
        URI remoteURI, String username, String password, boolean syncPublish) {

        return (TopicConnectionFactory) createConnectionFactory(remoteURI, username, password, syncPublish);
    }

    private ConnectionFactory createConnectionFactory(
        URI remoteURI, String username, String password, boolean syncPublish) {

        String clientScheme;
        boolean useSSL = false;

        switch (remoteURI.getScheme()) {
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

        String amqpURI = clientScheme + remoteURI.getHost() + ":" + remoteURI.getPort();

        if (useSSL) {
            amqpURI += "?transport.verifyHost=false";
        }

        LOG.debug("In createConnectionFactory using URI: {}", amqpURI);

        JmsConnectionFactory factory = new JmsConnectionFactory(amqpURI);

        factory.setUsername(username);
        factory.setPassword(password);
        factory.setForceSyncSend(syncPublish);
        factory.setTopicPrefix("topic://");
        factory.setQueuePrefix("queue://");
        factory.setCloseTimeout(60000);

        return factory;
    }
}
