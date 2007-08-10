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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.jms.ConnectionConsumer;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageDispatch;

/**
 * For application servers, <CODE>Connection</CODE> objects provide a special
 * facility for creating a <CODE>ConnectionConsumer</CODE> (optional). The
 * messages it is to consume are specified by a <CODE>Destination</CODE> and a
 * message selector. In addition, a <CODE>ConnectionConsumer</CODE> must be
 * given a <CODE>ServerSessionPool</CODE> to use for processing its messages.
 * <p/>
 * <P>
 * Normally, when traffic is light, a <CODE>ConnectionConsumer</CODE> gets a
 * <CODE>ServerSession</CODE> from its pool, loads it with a single message,
 * and starts it. As traffic picks up, messages can back up. If this happens, a
 * <CODE>ConnectionConsumer</CODE> can load each <CODE>ServerSession</CODE>
 * with more than one message. This reduces the thread context switches and
 * minimizes resource use at the expense of some serialization of message
 * processing.
 * 
 * @see javax.jms.Connection#createConnectionConsumer
 * @see javax.jms.Connection#createDurableConnectionConsumer
 * @see javax.jms.QueueConnection#createConnectionConsumer
 * @see javax.jms.TopicConnection#createConnectionConsumer
 * @see javax.jms.TopicConnection#createDurableConnectionConsumer
 */

public class ActiveMQConnectionConsumer implements ConnectionConsumer, ActiveMQDispatcher {

    private ActiveMQConnection connection;
    private ServerSessionPool sessionPool;
    private ConsumerInfo consumerInfo;
    private boolean closed;

    /**
     * Create a ConnectionConsumer
     * 
     * @param theConnection
     * @param theSessionPool
     * @param theConsumerInfo
     * @throws JMSException
     */
    protected ActiveMQConnectionConsumer(ActiveMQConnection theConnection, ServerSessionPool theSessionPool, ConsumerInfo theConsumerInfo) throws JMSException {
        this.connection = theConnection;
        this.sessionPool = theSessionPool;
        this.consumerInfo = theConsumerInfo;

        this.connection.addConnectionConsumer(this);
        this.connection.addDispatcher(consumerInfo.getConsumerId(), this);
        this.connection.syncSendPacket(this.consumerInfo);
    }

    /**
     * Gets the server session pool associated with this connection consumer.
     * 
     * @return the server session pool used by this connection consumer
     * @throws JMSException if the JMS provider fails to get the server session
     *                 pool associated with this consumer due to some internal
     *                 error.
     */

    public ServerSessionPool getServerSessionPool() throws JMSException {
        if (closed) {
            throw new IllegalStateException("The Connection Consumer is closed");
        }
        return this.sessionPool;
    }

    /**
     * Closes the connection consumer. <p/>
     * <P>
     * Since a provider may allocate some resources on behalf of a connection
     * consumer outside the Java virtual machine, clients should close these
     * resources when they are not needed. Relying on garbage collection to
     * eventually reclaim these resources may not be timely enough.
     * 
     * @throws JMSException
     */

    public void close() throws JMSException {
        if (!closed) {
            dispose();
            this.connection.asyncSendPacket(this.consumerInfo.createRemoveCommand());
        }

    }

    public void dispose() {
        if (!closed) {
            this.connection.removeDispatcher(consumerInfo.getConsumerId());
            this.connection.removeConnectionConsumer(this);
            closed = true;
        }
    }

    public void dispatch(MessageDispatch messageDispatch) {
        try {
            messageDispatch.setConsumer(this);

            ServerSession serverSession = sessionPool.getServerSession();
            Session s = serverSession.getSession();
            ActiveMQSession session = null;

            if (s instanceof ActiveMQSession) {
                session = (ActiveMQSession)s;
            } else if (s instanceof ActiveMQTopicSession) {
                ActiveMQTopicSession topicSession = (ActiveMQTopicSession)s;
                session = (ActiveMQSession)topicSession.getNext();
            } else if (s instanceof ActiveMQQueueSession) {
                ActiveMQQueueSession queueSession = (ActiveMQQueueSession)s;
                session = (ActiveMQSession)queueSession.getNext();
            } else {
                connection.onAsyncException(new JMSException("Session pool provided an invalid session type: " + s.getClass()));
                return;
            }

            session.dispatch(messageDispatch);
            serverSession.start();
        } catch (JMSException e) {
            connection.onAsyncException(e);
        }
    }

    public String toString() {
        return "ActiveMQConnectionConsumer { value=" + consumerInfo.getConsumerId() + " }";
    }
}
