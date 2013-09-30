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

import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;

import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;

/**
 * A factory of {@link XAConnection} instances
 * 
 * 
 */
public class ActiveMQXAConnectionFactory extends ActiveMQConnectionFactory implements XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory {

    public ActiveMQXAConnectionFactory() {
    }

    public ActiveMQXAConnectionFactory(String userName, String password, String brokerURL) {
        super(userName, password, brokerURL);
    }

    public ActiveMQXAConnectionFactory(String userName, String password, URI brokerURL) {
        super(userName, password, brokerURL);
    }

    public ActiveMQXAConnectionFactory(String brokerURL) {
        super(brokerURL);
    }

    public ActiveMQXAConnectionFactory(URI brokerURL) {
        super(brokerURL);
    }

    public XAConnection createXAConnection() throws JMSException {
        return (XAConnection) createActiveMQConnection();
    }

    public XAConnection createXAConnection(String userName, String password) throws JMSException {
        return (XAConnection) createActiveMQConnection(userName, password);
    }

    public XAQueueConnection createXAQueueConnection() throws JMSException {
        return (XAQueueConnection) createActiveMQConnection();
    }

    public XAQueueConnection createXAQueueConnection(String userName, String password) throws JMSException {
        return (XAQueueConnection) createActiveMQConnection(userName, password);
    }

    public XATopicConnection createXATopicConnection() throws JMSException {
        return (XATopicConnection) createActiveMQConnection();
    }

    public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException {
        return (XATopicConnection) createActiveMQConnection(userName, password);
    }

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
