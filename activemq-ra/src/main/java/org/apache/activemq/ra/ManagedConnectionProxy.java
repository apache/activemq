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

import java.util.ArrayList;
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQQueueSession;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQTopicSession;

/**
 * Acts as a pass through proxy for a JMS Connection object. It intercepts
 * events that are of interest of the ActiveMQManagedConnection.
 * 
 * @version $Revision$
 */
public class ManagedConnectionProxy implements Connection, QueueConnection, TopicConnection, ExceptionListener {

    private ActiveMQManagedConnection managedConnection;
    private ArrayList<ManagedSessionProxy> sessions = new ArrayList<ManagedSessionProxy>();
    private ExceptionListener exceptionListener;

    public ManagedConnectionProxy(ActiveMQManagedConnection managedConnection) {
        this.managedConnection = managedConnection;
    }

    /**
     * Used to let the ActiveMQManagedConnection that this connection handel is
     * not needed by the app.
     * 
     * @throws JMSException
     */
    public void close() throws JMSException {
        if (managedConnection != null) {
            managedConnection.proxyClosedEvent(this);
        }
    }

    /**
     * Called by the ActiveMQManagedConnection to invalidate this proxy.
     */
    public void cleanup() {
        exceptionListener = null;
        managedConnection = null;
        for (Iterator<ManagedSessionProxy> iter = sessions.iterator(); iter.hasNext();) {
            ManagedSessionProxy p = iter.next();
            try {
                p.cleanup();
            } catch (JMSException ignore) {
            }
            iter.remove();
        }
    }

    /**
     *
     * @return "physical" underlying activemq connection, if proxy is associated with a managed connection
     * @throws javax.jms.JMSException if managed connection is null
     */
    private Connection getConnection() throws JMSException {
        if (managedConnection == null) {
            throw new IllegalStateException("The Connection is closed");
        }
        return managedConnection.getPhysicalConnection();
    }

    /**
     * @param transacted Whether session is transacted
     * @param acknowledgeMode session acknowledge mode
     * @return session proxy
     * @throws JMSException on error
     */
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return createSessionProxy(transacted, acknowledgeMode);
    }

    /**
     * @param transacted Whether session is transacted
     * @param acknowledgeMode session acknowledge mode
     * @return session proxy
     * @throws JMSException on error
     */
    private ManagedSessionProxy createSessionProxy(boolean transacted, int acknowledgeMode) throws JMSException {
        if (!transacted && acknowledgeMode == Session.SESSION_TRANSACTED) {
            acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
        }
//        ActiveMQSession session = (ActiveMQSession)getConnection().createSession(true, acknowledgeMode);
        ActiveMQSession session = (ActiveMQSession)getConnection().createSession(transacted, acknowledgeMode);
        ManagedTransactionContext txContext = new ManagedTransactionContext(managedConnection.getTransactionContext());
        session.setTransactionContext(txContext);
        ManagedSessionProxy p = new ManagedSessionProxy(session);
        p.setUseSharedTxContext(managedConnection.isInManagedTx());
        sessions.add(p);
        return p;
    }

    public void setUseSharedTxContext(boolean enable) throws JMSException {
        for (ManagedSessionProxy p : sessions) {
            p.setUseSharedTxContext(enable);
        }
    }

    /**
     * @param transacted Whether session is transacted
     * @param acknowledgeMode session acknowledge mode
     * @return session proxy
     * @throws JMSException on error
     */
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQQueueSession(createSessionProxy(transacted, acknowledgeMode));
    }

    /**
     * @param transacted Whether session is transacted
     * @param acknowledgeMode session acknowledge mode
     * @return session proxy
     * @throws JMSException on error
     */
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQTopicSession(createSessionProxy(transacted, acknowledgeMode));
    }

    /**
     * @return
     * @throws JMSException
     */
    public String getClientID() throws JMSException {
        return getConnection().getClientID();
    }

    /**
     * @return
     * @throws JMSException
     */
    public ExceptionListener getExceptionListener() throws JMSException {
        return getConnection().getExceptionListener();
    }

    /**
     * @return
     * @throws JMSException
     */
    public ConnectionMetaData getMetaData() throws JMSException {
        return getConnection().getMetaData();
    }

    /**
     * @param clientID
     * @throws JMSException
     */
    public void setClientID(String clientID) throws JMSException {
        getConnection().setClientID(clientID);
    }

    /**
     * @param listener
     * @throws JMSException
     */
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        getConnection();
        exceptionListener = listener;
    }

    /**
     * @throws JMSException
     */
    public void start() throws JMSException {
        getConnection().start();
    }

    /**
     * @throws JMSException
     */
    public void stop() throws JMSException {
        getConnection().stop();
    }

    /**
     * @param queue
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return
     * @throws JMSException
     */
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("Not Supported.");
    }

    /**
     * @param topic
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return
     * @throws JMSException
     */
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("Not Supported.");
    }

    /**
     * @param destination
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return
     * @throws JMSException
     */
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("Not Supported.");
    }

    /**
     * @param topic
     * @param subscriptionName
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return
     * @throws JMSException
     */
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("Not Supported.");
    }

    /**
     * @return Returns the managedConnection.
     */
    public ActiveMQManagedConnection getManagedConnection() {
        return managedConnection;
    }

    public void onException(JMSException e) {
        if (exceptionListener != null && managedConnection != null) {
            try {
                exceptionListener.onException(e);
            } catch (Throwable ignore) {
                // We can never trust user code so ignore any exceptions.
            }
        }
    }

}
