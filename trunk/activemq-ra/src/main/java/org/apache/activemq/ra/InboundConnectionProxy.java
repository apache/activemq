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

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionMetaData;

/**
 * A {@link Connection} implementation which can be used with the ActiveMQ JCA
 * Resource Adapter to publish messages using the same JMS session that is used to dispatch
 * messages.
 *
 * 
 */
public class InboundConnectionProxy implements Connection, QueueConnection, TopicConnection {

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        // TODO we could decide to barf if someone passes in incompatible options
        return new InboundSessionProxy();
    }

    public QueueSession createQueueSession(boolean transacted, int ackMode) throws JMSException {
        // TODO we could decide to barf if someone passes in incompatible options
        return new InboundSessionProxy();
    }

    public TopicSession createTopicSession(boolean transacted, int ackMode) throws JMSException {
        // TODO we could decide to barf if someone passes in incompatible options
        return new InboundSessionProxy();
    }

    public void start() throws JMSException {
        // the JCA RA is in control of this
    }

    public void stop() throws JMSException {
        // the JCA RA is in control of this
    }

    public void close() throws JMSException {
        // the JCA RA is in control of this
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        return ActiveMQConnectionMetaData.INSTANCE;
    }

    public String getClientID() throws JMSException {
        throw createNotSupported("getClientID()");
    }

    public void setClientID(String s) throws JMSException {
        throw createNotSupported("setClient()");
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        throw createNotSupported("getExceptionListener()");
    }

    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        throw createNotSupported("setExceptionListener()");
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw createNotSupported("createConnectionConsumer()");
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw createNotSupported("createDurableConnectionConsumer()");
    }

    public ConnectionConsumer createConnectionConsumer(Queue queue, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw createNotSupported("createConnectionConsumer()");
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw createNotSupported("createConnectionConsumer()");
    }

    protected JMSException createNotSupported(String text) {
        return new JMSException("Operation: " + text + " is not supported for this proxy JCA ResourceAdapter provider");
    }
}
