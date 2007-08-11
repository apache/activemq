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

import java.util.Iterator;
import java.util.List;

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @version $Revision$ $Date$
 */
public class ServerSessionPoolImpl implements ServerSessionPool {
    
    private static final Log log = LogFactory.getLog(ServerSessionPoolImpl.class);

    private final ActiveMQEndpointWorker activeMQAsfEndpointWorker;
    private final int maxSessions;

    private List idleSessions = new CopyOnWriteArrayList();
    private List activeSessions = new CopyOnWriteArrayList();
    private AtomicBoolean closing = new AtomicBoolean(false);

    public ServerSessionPoolImpl(ActiveMQEndpointWorker activeMQAsfEndpointWorker, int maxSessions) {
        this.activeMQAsfEndpointWorker = activeMQAsfEndpointWorker;
        this.maxSessions=maxSessions;
    }

    private ServerSessionImpl createServerSessionImpl() throws JMSException {
        MessageActivationSpec activationSpec = activeMQAsfEndpointWorker.endpointActivationKey.getActivationSpec();
        int acknowledge = (activeMQAsfEndpointWorker.transacted) ? Session.SESSION_TRANSACTED : activationSpec.getAcknowledgeModeForSession();
        final ActiveMQSession session = (ActiveMQSession) activeMQAsfEndpointWorker.connection.createSession(activeMQAsfEndpointWorker.transacted,acknowledge);            
        MessageEndpoint endpoint;
        try {                
            int batchSize = 0;
            if (activationSpec.getEnableBatchBooleanValue()) {
                batchSize = activationSpec.getMaxMessagesPerBatchIntValue();
            }
            if( activationSpec.isUseRAManagedTransactionEnabled() ) {
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
            log.debug("Could not create an endpoint.", e);
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
        log.debug("ServerSession requested.");
        if (closing.get()) {
            throw new JMSException("Session Pool Shutting Down.");
        }

        if (idleSessions.size() > 0) {
            ServerSessionImpl ss = (ServerSessionImpl) idleSessions.remove(idleSessions.size() - 1);
            activeSessions.add(ss);
            log.debug("Using idle session: " + ss);
            return ss;
        } else {
            // Are we at the upper limit?
            if (activeSessions.size() >= maxSessions) {
                // then reuse the already created sessions..
                // This is going to queue up messages into a session for
                // processing.
                return getExistingServerSession();
            }
            ServerSessionImpl ss = createServerSessionImpl();
            // We may not be able to create a session due to the container
            // restricting us.
            if (ss == null) {
                if (idleSessions.size() == 0) {
                    throw new JMSException("Endpoint factory did not allows to any endpoints.");
                }

                return getExistingServerSession();
            }
            activeSessions.add(ss);
            log.debug("Created a new session: " + ss);
            return ss;
        }
    }

    /**
     * @param messageDispatch the message to dispatch
     * @throws JMSException
     */
    private void dispatchToSession(MessageDispatch messageDispatch) throws JMSException {

        ServerSession serverSession = getServerSession();
        Session s = serverSession.getSession();
        ActiveMQSession session = null;
        if( s instanceof ActiveMQSession ) {
            session = (ActiveMQSession) s;
        } else if(s instanceof ActiveMQQueueSession) {
            session = (ActiveMQSession) s;
        } else if(s instanceof ActiveMQTopicSession) {
            session = (ActiveMQSession) s;
        } else {
        	activeMQAsfEndpointWorker.connection.onAsyncException(new JMSException("Session pool provided an invalid session type: "+s.getClass()));
        }
        session.dispatch(messageDispatch);
        serverSession.start();
    }

    
    /**
     * @return
     */
    private ServerSession getExistingServerSession() {
        ServerSessionImpl ss = (ServerSessionImpl) activeSessions.remove(0);
        activeSessions.add(ss);
        log.debug("Reusing an active session: " + ss);
        return ss;
    }

    public void returnToPool(ServerSessionImpl ss) {
        log.debug("Session returned to pool: " + ss);
        activeSessions.remove(ss);
        idleSessions.add(ss);
        synchronized(closing){
            closing.notify();
        }
    }

     public void removeFromPool(ServerSessionImpl ss) {
        activeSessions.remove(ss);
        try {
            ActiveMQSession session = (ActiveMQSession) ss.getSession();
            List l = session.getUnconsumedMessages();
            for (Iterator i = l.iterator(); i.hasNext();) {
                dispatchToSession((MessageDispatch) i.next());        			
            }
        } catch (Throwable t) {
            log.error("Error redispatching unconsumed messages from stale session", t);    	
        }
        ss.close();
        synchronized(closing){
            closing.notify();
        }
    }

    public void close() {
        synchronized (closing) {
            closing.set(true);
            closeIdleSessions();
            while( activeSessions.size() > 0 ) {
                System.out.println("ACtive Sessions = " + activeSessions.size());
                try {
                    closing.wait(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                closeIdleSessions();
            }
        }
    }

    private void closeIdleSessions() {
        for (Iterator iter = idleSessions.iterator(); iter.hasNext();) {
            ServerSessionImpl ss = (ServerSessionImpl) iter.next();
            ss.close();
        }
    }

    /**
     * @return Returns the closing.
     */
    public boolean isClosing(){
        return closing.get();
    }

    /**
     * @param closing The closing to set.
     */
    public void setClosing(boolean closing){
        this.closing.set(closing);
    }

}
