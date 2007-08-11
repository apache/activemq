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
package org.apache.activemq.systest.impl;

import org.apache.activemq.systest.AgentStopper;
import org.apache.activemq.systest.AgentSupport;
import org.apache.activemq.systest.BrokerAgent;
import org.apache.activemq.systest.ClientAgent;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * Simple base class for JMS client agents.
 * 
 * @version $Revision: 1.1 $
 */
public class JmsClientSupport extends AgentSupport implements ClientAgent {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private boolean transacted;
    private int acknowledgementMode = Session.CLIENT_ACKNOWLEDGE;
    private Destination destination;

    public void start() throws Exception {
        getConnection().start();
    }

    public void connectTo(BrokerAgent broker) throws Exception {
        // lets stop the connection then start a new connection
        stop();
        setConnectionFactory(broker.getConnectionFactory());
        start();
    }

    // Properties
    // -------------------------------------------------------------------------
    public Destination getDestination() {
        if (destination == null) {
            throw new IllegalArgumentException("You must set the 'destination' property");
        }
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public int getAcknowledgementMode() {
        return acknowledgementMode;
    }

    public void setAcknowledgementMode(int acknowledgementMode) {
        this.acknowledgementMode = acknowledgementMode;
    }

    public ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("You must set the 'connectionFactory' property");
        }
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public boolean isTransacted() {
        return transacted;
    }

    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    public Connection getConnection() throws JMSException {
        if (connection == null) {
            connection = createConnection();
        }
        return connection;
    }

    public Session getSession() throws JMSException {
        if (session == null) {
            session = createSession();
        }
        return session;
    }

    public void stop(AgentStopper stopper) {
        if (session != null) {
            try {
                session.close();
            }
            catch (JMSException e) {
                stopper.onException(this, e);
            }
            finally {
                session = null;
            }
        }
        if (connection != null) {
            try {
                connection.close();
            }
            catch (JMSException e) {
                stopper.onException(this, e);
            }
            finally {
                connection = null;
            }
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Connection createConnection() throws JMSException {
        return getConnectionFactory().createConnection();
    }

    protected Session createSession() throws JMSException {
        return getConnection().createSession(transacted, acknowledgementMode);
    }

}
