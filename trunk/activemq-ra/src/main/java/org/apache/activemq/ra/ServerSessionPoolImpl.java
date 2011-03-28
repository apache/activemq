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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  $Date$
 */
public class ServerSessionPoolImpl implements ServerSessionPool {

    private static final Logger LOG = LoggerFactory.getLogger(ServerSessionPoolImpl.class);

    private final ActiveMQEndpointWorker activeMQAsfEndpointWorker;
    private final int maxSessions;

    private final List<ServerSessionImpl> idleSessions = new ArrayList<ServerSessionImpl>();
    private final List<ServerSessionImpl> activeSessions = new ArrayList<ServerSessionImpl>();
    private final Lock sessionLock = new ReentrantLock();
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public ServerSessionPoolImpl(ActiveMQEndpointWorker activeMQAsfEndpointWorker, int maxSessions) {
        this.activeMQAsfEndpointWorker = activeMQAsfEndpointWorker;
        this.maxSessions = maxSessions;
    }

    private ServerSessionImpl createServerSessionImpl() throws JMSException {
        MessageActivationSpec activationSpec = activeMQAsfEndpointWorker.endpointActivationKey.getActivationSpec();
        int acknowledge = (activeMQAsfEndpointWorker.transacted) ? Session.SESSION_TRANSACTED : activationSpec.getAcknowledgeModeForSession();
        final ActiveMQSession session = (ActiveMQSession)activeMQAsfEndpointWorker.getConnection().createSession(activeMQAsfEndpointWorker.transacted, acknowledge);
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
        sessionLock.lock();
        try {
            ss = getExistingServerSession(false);
        } finally {
            sessionLock.unlock();
        }
        if (ss != null) {
            return ss;
        }
        ss = createServerSessionImpl();
        sessionLock.lock();
        try {
            // We may not be able to create a session due to the container
            // restricting us.
            if (ss == null) {
                if (activeSessions.isEmpty() && idleSessions.isEmpty()) {
                    throw new JMSException("Endpoint factory did not allow creation of any endpoints.");
                }

                ss = getExistingServerSession(true);
            } else {
                activeSessions.add(ss);
            }
        } finally {
            sessionLock.unlock();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created a new session: " + ss);
        }
        return ss;

    }

    /**
     * Must be called with sessionLock held.
     * Returns an idle session if one exists or an active session if no more
     * sessions can be created.  Sessions can not be created if force is true
     * or activeSessions >= maxSessions.
     * @param force do not check activeSessions >= maxSessions, return an active connection anyway.
     * @return an already existing session.
     */
    private ServerSessionImpl getExistingServerSession(boolean force) {
        ServerSessionImpl ss = null;
        if (idleSessions.size() > 0) {
            ss = idleSessions.remove(idleSessions.size() - 1);
        }
        if (ss != null) {
            activeSessions.add(ss);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using idle session: " + ss);
            }
        } else if (force || activeSessions.size() >= maxSessions) {
            // If we are at the upper limit
            // then reuse the already created sessions..
            // This is going to queue up messages into a session for
            // processing.
            ss = getExistingActiveServerSession();
        }
        return ss;
    }

    /**
     * Must be called with sessionLock held.
     * Returns the first session from activeSessions, shifting it to last.
     * @return session
     */
    private ServerSessionImpl getExistingActiveServerSession() {
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
        sessionLock.lock();
            activeSessions.remove(ss);
        try {
            // make sure we only return non-stale sessions to the pool
            if ( ss.isStale() ) {
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug("Discarding stale ServerSession to be returned to pool: " + ss);
                }
                ss.close();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ServerSession returned to pool: " + ss);
                }
            idleSessions.add(ss);
            }
        } finally {
            sessionLock.unlock();
        }
        synchronized (closing) {
            closing.notify();
        }
    }

    public void removeFromPool(ServerSessionImpl ss) {
        sessionLock.lock();
        try {
            activeSessions.remove(ss);
        } finally {
            sessionLock.unlock();
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
            activeMQAsfEndpointWorker.getConnection()
                    .onAsyncException(new JMSException(
                            "Session pool provided an invalid session type: "
                                    + s.getClass()));
        }
        session.dispatch(messageDispatch);
        serverSession.start();
    }

    public void close() {
        closing.set(true);
        int activeCount = closeIdleSessions();
        // we may have to wait erroneously 250ms if an
        // active session is removed during our wait and we
        // are not notified
        while (activeCount > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Active Sessions = " + activeCount);
            }
            try {
                synchronized (closing) {
                    closing.wait(250);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            activeCount = closeIdleSessions();
        }
    }


    protected int closeIdleSessions() {
        sessionLock.lock();
        try {
            for (ServerSessionImpl ss : idleSessions) {
                ss.close();
            }
            idleSessions.clear();
            return activeSessions.size();
        } finally {
            sessionLock.unlock();
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

}
