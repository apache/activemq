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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.endpoint.MessageEndpoint;

import org.apache.activemq.ActiveMQQueueSession;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQTopicSession;
import org.apache.activemq.command.MessageDispatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$ $Date$
 */
public class ServerSessionPoolImpl implements ServerSessionPool {

    private static final Log LOG = LogFactory.getLog(ServerSessionPoolImpl.class);

    private final ActiveMQEndpointWorker activeMQAsfEndpointWorker;
    private final int maxSessions;

    private List<ServerSessionImpl> idleSessions = new ArrayList<ServerSessionImpl>();
    private List<ServerSessionImpl> activeSessions = new ArrayList<ServerSessionImpl>();
    private AtomicBoolean closing = new AtomicBoolean(false);

    public ServerSessionPoolImpl(ActiveMQEndpointWorker activeMQAsfEndpointWorker, int maxSessions) {
        this.activeMQAsfEndpointWorker = activeMQAsfEndpointWorker;
        this.maxSessions = maxSessions;
    }

    private ServerSessionImpl createServerSessionImpl() throws JMSException {
        MessageActivationSpec activationSpec = activeMQAsfEndpointWorker.endpointActivationKey.getActivationSpec();
        int acknowledge = (activeMQAsfEndpointWorker.transacted) ? Session.SESSION_TRANSACTED : activationSpec.getAcknowledgeModeForSession();
        final ActiveMQSession session = (ActiveMQSession)activeMQAsfEndpointWorker.connection.createSession(activeMQAsfEndpointWorker.transacted, acknowledge);
        MessageEndpoint endpoint;
        try {
            int batchSize = 0;
            if (activationSpec.getEnableBatchBooleanValue()) {
                batchSize = activationSpec.getMaxMessagesPerBatchIntValue();
            }
            if (activationSpec.isUseRAManagedTransactionEnabled()) {
                // The RA will manage the transaction commit.
                endpoint = createEndpoint(null);
                return new ServerSessionImpl(this, (ActiveMQSession)session, activeMQAsfEndpointWorker.workManager, endpoint, true, batchSize);
            } else {
                // Give the container an object to manage to transaction with.
                endpoint = createEndpoint(new LocalAndXATransaction(session.getTransactionContext()));
                return new ServerSessionImpl(this, (ActiveMQSession)session, activeMQAsfEndpointWorker.workManager, endpoint, false, batchSize);
            }
        } catch (UnavailableException e) {
            // The container could be limiting us on the number of endpoints
            // that are being created.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Could not create an endpoint.", e);
            }
            session.close();
            return null;
        }
    }

    private MessageEndpoint createEndpoint(LocalAndXATransaction txResourceProxy) throws UnavailableException {
        MessageEndpoint endpoint;
        endpoint = activeMQAsfEndpointWorker.endpointFactory.createEndpoint(txResourceProxy);
        MessageEndpointProxy endpointProxy = new MessageEndpointProxy(endpoint);
        return endpointProxy;
    }

    /**
     */
    public ServerSession getServerSession() throws JMSException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("ServerSession requested.");
        }
        if (closing.get()) {
            throw new JMSException("Session Pool Shutting Down.");
        }

        ServerSessionImpl ss = null;
        synchronized (idleSessions) {
            if (idleSessions.size() > 0) {
                ss = idleSessions.remove(idleSessions.size() - 1);
            }
        }
        if (ss != null) {
            synchronized (activeSessions) {
                activeSessions.add(ss);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using idle session: " + ss);
            }
            return ss;
        }

        synchronized (activeSessions) {
            // Are we at the upper limit?
            if (activeSessions.size() >= maxSessions) {
                // then reuse the already created sessions..
                // This is going to queue up messages into a session for
                // processing.
                return getExistingServerSession();
            }
        }

        ss = createServerSessionImpl();
        // We may not be able to create a session due to the container
        // restricting us.
        if (ss == null) {
            synchronized (activeSessions) {
                if (activeSessions.isEmpty()) {
                    throw new JMSException(
                            "Endpoint factory did not allow creation any endpoints.");
                }
            }

            return getExistingServerSession();
        }
        synchronized (activeSessions) {
            activeSessions.add(ss);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created a new session: " + ss);
        }
        return ss;

    }

    /**
     * @param messageDispatch
     *            the message to dispatch
     * @throws JMSException
     */
    private void dispatchToSession(MessageDispatch messageDispatch)
            throws JMSException {

        ServerSession serverSession = getServerSession();
        Session s = serverSession.getSession();
        ActiveMQSession session = null;
        if (s instanceof ActiveMQSession) {
            session = (ActiveMQSession) s;
        } else if (s instanceof ActiveMQQueueSession) {
            session = (ActiveMQSession) s;
        } else if (s instanceof ActiveMQTopicSession) {
            session = (ActiveMQSession) s;
        } else {
            activeMQAsfEndpointWorker.connection
                    .onAsyncException(new JMSException(
                            "Session pool provided an invalid session type: "
                                    + s.getClass()));
        }
        session.dispatch(messageDispatch);
        serverSession.start();
    }

    /**
     * @return session
     */
    private ServerSession getExistingServerSession() {
        ServerSessionImpl ss = null;
        if (!activeSessions.isEmpty()) {
            if (activeSessions.size() > 1) {
                // round robin
                ss = activeSessions.remove(0);
                activeSessions.add(ss);
            } else {
                ss = activeSessions.get(0);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reusing an active session: " + ss);
        }
        return ss;
    }

    public void returnToPool(ServerSessionImpl ss) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Session returned to pool: " + ss);
        }
        synchronized(activeSessions) {
            activeSessions.remove(ss);
        }
        synchronized(idleSessions) {
            idleSessions.add(ss);
        }
        synchronized (closing) {
            closing.notify();
        }
    }

    public void removeFromPool(ServerSessionImpl ss) {
        synchronized(activeSessions) {
            activeSessions.remove(ss);
        }
        try {
            ActiveMQSession session = (ActiveMQSession)ss.getSession();
            List l = session.getUnconsumedMessages();
            for (Iterator i = l.iterator(); i.hasNext();) {
                dispatchToSession((MessageDispatch)i.next());
            }
        } catch (Throwable t) {
            LOG.error("Error redispatching unconsumed messages from stale session", t);
        }
        ss.close();
        synchronized (closing) {
            closing.notify();
        }
    }

    public void close() {
        closing.set(true);
        closeIdleSessions();
        // we may have to wait erroneously 250ms if an
        // active session is removed during our wait and we
        // are not notified
        while (getActiveSessionSize() > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Active Sessions = " + getActiveSessionSize());
            }
            try {
                synchronized (closing) {
                    closing.wait(250);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            closeIdleSessions();
        }
    }
    

    private void closeIdleSessions() {
        synchronized(idleSessions) {
            for (Iterator<ServerSessionImpl> iter = idleSessions.iterator(); iter.hasNext();) {
                ServerSessionImpl ss = iter.next();
                ss.close();
            }
            idleSessions.clear();
        }
    }

    /**
     * @return Returns the closing.
     */
    public boolean isClosing() {
        return closing.get();
    }
    
    /**
     * @param closing The closing to set.
     */
    public void setClosing(boolean closing) {
        this.closing.set(closing);
    }
    
    private int getActiveSessionSize() {
        synchronized(activeSessions) {
            return activeSessions.size();
        }
    }

}
