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
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context used for AMQP JMS Clients to create connection instances.
 */
public class JmsClientContext {

    private static final Logger LOG = LoggerFactory.getLogger(JmsClientContext.class);

    public static final JmsClientContext INSTANCE = new JmsClientContext();

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
        ConnectionFactoryImpl factory = createConnectionFactory(remoteURI, username, password, clientId, syncPublish);

        return factory.createConnection();
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
        ConnectionFactoryImpl factory = createConnectionFactory(remoteURI, username, password, clientId, syncPublish);

        return factory.createTopicConnection();
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
        ConnectionFactoryImpl factory = createConnectionFactory(remoteURI, username, password, clientId, syncPublish);

        return factory.createQueueConnection();
    }

    //------ Internal Implementation bits ------------------------------------//

    private ConnectionFactoryImpl createConnectionFactory(
        URI remoteURI, String username, String password, String clientId, boolean syncPublish) {

        boolean useSSL = remoteURI.getScheme().toLowerCase().contains("ssl");

        LOG.debug("In createConnectionFactory using port {} ssl? {}", remoteURI.getPort(), useSSL);

        ConnectionFactoryImpl factory =
            new ConnectionFactoryImpl(remoteURI.getHost(), remoteURI.getPort(), username, password, clientId, useSSL);

        if (useSSL) {
            factory.setKeyStorePath(System.getProperty("javax.net.ssl.trustStore"));
            factory.setKeyStorePassword("password");
            factory.setTrustStorePath(System.getProperty("javax.net.ssl.keyStore"));
            factory.setTrustStorePassword("password");
        }

        factory.setTopicPrefix("topic://");
        factory.setQueuePrefix("queue://");
        factory.setSyncPublish(syncPublish);

        return factory;
    }
}
