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

import org.apache.activemq.Service;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;

/**
 * An agent which listens to commands on a JMS destination
 *
 * @org.apache.xbean.XBean
 *
 * @version $Revision: $
 */
public class CommandAgent implements Service, BrokerServiceAware {
    private static final Log log = LogFactory.getLog(CommandAgent.class);

    private String brokerUrl = "vm://localhost";
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Destination commandDestination;
    private CommandMessageListener listener;
    private Session session;
    private MessageConsumer consumer;
    private String brokerName = "default";

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
        consumer.close();
        consumer = null;
        session.close();
        session = null;
        connection.close();
        connection = null;
    }

    public void setBrokerService(BrokerService brokerService) {
        String name = brokerService.getBrokerName();
        if (name != null) {
            brokerName = name;
        }
    }

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
        return AdvisorySupport.getAgentDestination(brokerName); 
    }
}
