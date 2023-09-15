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
package org.apache.activemq;

import java.net.URI;
import java.util.Properties;

import jakarta.jms.JMSException;
import jakarta.jms.XAConnection;
import jakarta.jms.XAConnectionFactory;
import jakarta.jms.XAJMSContext;
import jakarta.jms.XAQueueConnection;
import jakarta.jms.XAQueueConnectionFactory;
import jakarta.jms.XATopicConnection;
import jakarta.jms.XATopicConnectionFactory;

import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.JMSExceptionSupport;

public class ActiveMQXASslConnectionFactory extends ActiveMQSslConnectionFactory implements XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory {

    public ActiveMQXASslConnectionFactory() {
    }

    public ActiveMQXASslConnectionFactory(String brokerURL) {
        super(brokerURL);
    }

    public ActiveMQXASslConnectionFactory(URI brokerURL) {
        super(brokerURL);
    }

    @Override
    public XAConnection createXAConnection() throws JMSException {
        return (XAConnection) createActiveMQConnection();
    }

    @Override
    public XAConnection createXAConnection(String userName, String password) throws JMSException {
        return (XAConnection) createActiveMQConnection(userName, password);
    }

    @Override
    public XAQueueConnection createXAQueueConnection() throws JMSException {
        return (XAQueueConnection) createActiveMQConnection();
    }

    @Override
    public XAQueueConnection createXAQueueConnection(String userName, String password) throws JMSException {
        return (XAQueueConnection) createActiveMQConnection(userName, password);
    }

    @Override
    public XATopicConnection createXATopicConnection() throws JMSException {
        return (XATopicConnection) createActiveMQConnection();
    }

    @Override
    public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException {
        return (XATopicConnection) createActiveMQConnection(userName, password);
    }

    @Override
    public XAJMSContext createXAContext() {
        try {
            return new ActiveMQXAContext((ActiveMQXAConnection)createXAConnection());
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public XAJMSContext createXAContext(String userName, String password) {
        try {
            return new ActiveMQXAContext((ActiveMQXAConnection)createXAConnection(userName, password));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    protected ActiveMQConnection createActiveMQConnection(Transport transport, JMSStatsImpl stats) throws Exception {
        ActiveMQXAConnection connection = new ActiveMQXAConnection(transport, getClientIdGenerator(), getConnectionIdGenerator(), stats);
        configureXAConnection(connection);
        return connection;
    }

    private void configureXAConnection(ActiveMQXAConnection connection) {
        connection.setXaAckMode(xaAckMode);
    }

    public int getXaAckMode() {
        return xaAckMode;
    }

    public void setXaAckMode(int xaAckMode) {
        this.xaAckMode = xaAckMode;
    }

    @Override
    public void populateProperties(Properties props) {
        super.populateProperties(props);
        props.put("xaAckMode", Integer.toString(xaAckMode));
    }
}
