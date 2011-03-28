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
package org.apache.activemq.ra;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Reference;
import javax.resource.Referenceable;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ActiveMQConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, Referenceable, Serializable {

    private static final long serialVersionUID = -5754338187296859149L;

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConnectionFactory.class);
    private ConnectionManager manager;
    private ActiveMQManagedConnectionFactory factory;
    private Reference reference;
    private final ActiveMQConnectionRequestInfo info;

    /**
     * @param factory
     * @param manager
     * @param connectionRequestInfo
     */
    public ActiveMQConnectionFactory(
            ActiveMQManagedConnectionFactory factory, 
            ConnectionManager manager, 
            ActiveMQConnectionRequestInfo connectionRequestInfo) {
        this.factory = factory;
        this.manager = manager;
        this.info = connectionRequestInfo;
    }

    /**
     * @see javax.jms.ConnectionFactory#createConnection()
     */
    public Connection createConnection() throws JMSException {
        return createConnection(info.copy());
    }

    /**
     * @see javax.jms.ConnectionFactory#createConnection(java.lang.String,
     *      java.lang.String)
     */
    public Connection createConnection(String userName, String password) throws JMSException {
        ActiveMQConnectionRequestInfo i = info.copy();
        i.setUserName(userName);
        i.setPassword(password);
        return createConnection(i);
    }

    /**
     * @param connectionRequestInfo
     * @return
     * @throws JMSException
     */
    private Connection createConnection(ActiveMQConnectionRequestInfo connectionRequestInfo) throws JMSException {
        try {
            if (connectionRequestInfo.isUseInboundSessionEnabled()) {
                return new InboundConnectionProxy();
            }
            if (manager == null) {
                throw new JMSException("No JCA ConnectionManager configured! Either enable UseInboundSessionEnabled or get your JCA container to configure one.");
            }
            return (Connection)manager.allocateConnection(factory, connectionRequestInfo);
        } catch (ResourceException e) {
            // Throw the root cause if it was a JMSException..
            if (e.getCause() instanceof JMSException) {
                throw (JMSException)e.getCause();
            }
            LOG.debug("Connection could not be created:", e);
            throw new JMSException(e.getMessage());
        }
    }

    /**
     * @see javax.naming.Referenceable#getReference()
     */
    public Reference getReference() {
        return reference;
    }

    /**
     * @see javax.resource.Referenceable#setReference(javax.naming.Reference)
     */
    public void setReference(Reference reference) {
        this.reference = reference;
    }

    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection)createConnection();
    }

    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection)createConnection(userName, password);
    }

    public TopicConnection createTopicConnection() throws JMSException {
        return (TopicConnection)createConnection();
    }

    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return (TopicConnection)createConnection(userName, password);
    }
}
