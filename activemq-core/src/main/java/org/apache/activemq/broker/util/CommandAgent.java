/*
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
package org.apache.activemq.broker.util;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * An agent which listens to commands on a JMS destination
 * 
 * @version $Revision$
 * @org.apache.xbean.XBean
 */
public class CommandAgent implements Service, InitializingBean, DisposableBean, FactoryBean {
    private static final Log log = LogFactory.getLog(CommandAgent.class);

    private String brokerUrl = "vm://localhost";
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Destination commandDestination;
    private CommandMessageListener listener;
    private Session session;
    private MessageConsumer consumer;

    public void start() throws Exception {
        session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        listener = new CommandMessageListener(session);
        Destination destination = getCommandDestination();
        if (log.isDebugEnabled()) {
            log.debug("Agent subscribing to control destination: " + destination);
        }
        consumer = session.createConsumer(destination);
        consumer.setMessageListener(listener);
    }

    public void stop() throws Exception {
        ServiceStopper stopper = new ServiceStopper();
        if (consumer != null) {
            try {
                consumer.close();
                consumer = null;
            } catch (JMSException e) {
                stopper.onException(this, e);
            }
        }
        if (session != null) {
            try {
                session.close();
                session = null;
            } catch (JMSException e) {
                stopper.onException(this, e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (JMSException e) {
                stopper.onException(this, e);
            }
        }
        stopper.throwFirstException();
    }

    // the following methods ensure that we are created on startup and the
    // lifecycles respected
    // TODO there must be a simpler way?
    public void afterPropertiesSet() throws Exception {
        start();
    }

    public void destroy() throws Exception {
        stop();
    }

    public Object getObject() throws Exception {
        return this;
    }

    public Class getObjectType() {
        return getClass();
    }

    public boolean isSingleton() {
        return true;
    }

    // Properties
    // -------------------------------------------------------------------------
    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        }
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Connection getConnection() throws JMSException {
        if (connection == null) {
            connection = createConnection();
            connection.start();
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Destination getCommandDestination() {
        if (commandDestination == null) {
            commandDestination = createCommandDestination();
        }
        return commandDestination;
    }

    public void setCommandDestination(Destination commandDestination) {
        this.commandDestination = commandDestination;
    }

    protected Connection createConnection() throws JMSException {
        return getConnectionFactory().createConnection();
    }

    protected Destination createCommandDestination() {
        return AdvisorySupport.getAgentDestination();
    }
}
