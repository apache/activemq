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

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.XAConnection;
import jakarta.jms.XAQueueConnection;
import jakarta.jms.XAQueueSession;
import jakarta.jms.XASession;
import jakarta.jms.XATopicConnection;
import jakarta.jms.XATopicSession;

import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IdGenerator;

/**
 * The XAConnection interface extends the capability of Connection by providing
 * an XASession (optional).
 * <p/>
 * The XAConnection interface is optional. JMS providers are not required to
 * support this interface. This interface is for use by JMS providers to
 * support transactional environments. Client programs are strongly encouraged
 * to use the transactional support  available in their environment, rather
 * than use these XA  interfaces directly.
 *
 * 
 * @see jakarta.jms.Connection
 * @see jakarta.jms.ConnectionFactory
 * @see jakarta.jms.QueueConnection
 * @see jakarta.jms.TopicConnection
 * @see jakarta.jms.TopicConnectionFactory
 * @see jakarta.jms.QueueConnection
 * @see jakarta.jms.QueueConnectionFactory
 */
public class ActiveMQXAConnection extends ActiveMQConnection implements XATopicConnection, XAQueueConnection, XAConnection {

    private int xaAckMode;

    protected ActiveMQXAConnection(Transport transport, IdGenerator clientIdGenerator,
                                   IdGenerator connectionIdGenerator, JMSStatsImpl factoryStats) throws Exception {
        super(transport, clientIdGenerator, connectionIdGenerator, factoryStats);
    }

    public XASession createXASession() throws JMSException {
        return (XASession) createSession(true, Session.SESSION_TRANSACTED);
    }

    public XATopicSession createXATopicSession() throws JMSException {
        return (XATopicSession) createSession(true, Session.SESSION_TRANSACTED);
    }

    public XAQueueSession createXAQueueSession() throws JMSException {
        return (XAQueueSession) createSession(true, Session.SESSION_TRANSACTED);
    }

    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        return new ActiveMQXASession(this, getNextSessionId(), getAckMode(), isDispatchAsync());
    }

    private int getAckMode() {
        return xaAckMode > 0 ? xaAckMode : Session.SESSION_TRANSACTED;
    }

    public void setXaAckMode(int xaAckMode) {
        this.xaAckMode = xaAckMode;
    }

    public int getXaAckMode() {
        return xaAckMode;
    }
}
