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
package org.apache.activemq.broker.util;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An agent which listens to commands on a JMS destination
 * 
 * 
 * @org.apache.xbean.XBean
 */
public class CommandAgent implements Service, ExceptionListener {
    private static final Logger LOG = LoggerFactory.getLogger(CommandAgent.class);

    private String brokerUrl = "vm://localhost";
    private String username;
    private String password;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Destination commandDestination;
    private CommandMessageListener listener;
    private Session session;
    private MessageConsumer consumer;

    /**
     *
     * @throws Exception
     * @org.apache.xbean.InitMethod
     */
    @PostConstruct
    public void start() throws Exception {
        session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        listener = new CommandMessageListener(session);
        Destination destination = getCommandDestination();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Agent subscribing to control destination: " + destination);
        }
        consumer = session.createConsumer(destination);
        consumer.setMessageListener(listener);
    }

    /**
     *
     * @throws Exception
     * @org.apache.xbean.DestroyMethod
     */
    @PreDestroy
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

    // Properties
    // -------------------------------------------------------------------------
    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }    

    public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
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
            connection.setExceptionListener(this);
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
        return getConnectionFactory().createConnection(username, password);
    }

    protected Destination createCommandDestination() {
        return AdvisorySupport.getAgentDestination();
    }

    public void onException(JMSException exception) {
        try {
            stop();
        } catch (Exception e) {
        }
    }
}
