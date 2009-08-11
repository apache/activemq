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

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.LocalTransactionEventListener;
import org.apache.activemq.TransactionContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ActiveMQManagedConnection maps to real physical connection to the server.
 * Since a ManagedConnection has to provide a transaction managment interface to
 * the physical connection, and sessions are the objects implement transaction
 * managment interfaces in the JMS API, this object also maps to a singe
 * physical JMS session. <p/> The side-effect is that JMS connection the
 * application gets will allways create the same session object. This is good if
 * running in an app server since the sessions are elisted in the context
 * transaction. This is bad if used outside of an app server since the user may
 * be trying to create 2 different sessions to coordinate 2 different uow.
 * 
 * @version $Revision$
 */
public class ActiveMQManagedConnection implements ManagedConnection, ExceptionListener { // TODO:
                                                                                            // ,
                                                                                            // DissociatableManagedConnection
                                                                                            // {

    private static final Log LOG = LogFactory.getLog(ActiveMQManagedConnection.class);

    private PrintWriter logWriter;

    private final ActiveMQConnection physicalConnection;
    private final TransactionContext transactionContext;
    private final List<ManagedConnectionProxy> proxyConnections = new CopyOnWriteArrayList<ManagedConnectionProxy>();
    private final List<ConnectionEventListener> listeners = new CopyOnWriteArrayList<ConnectionEventListener>();
    private final LocalAndXATransaction localAndXATransaction;

    private Subject subject;
    private ActiveMQConnectionRequestInfo info;
    private boolean destroyed;

    public ActiveMQManagedConnection(Subject subject, ActiveMQConnection physicalConnection, ActiveMQConnectionRequestInfo info) throws ResourceException {
        try {
            this.subject = subject;
            this.info = info;
            this.physicalConnection = physicalConnection;
            this.transactionContext = new TransactionContext(physicalConnection);

            this.localAndXATransaction = new LocalAndXATransaction(transactionContext) {
                public void setInManagedTx(boolean inManagedTx) throws JMSException {
                    super.setInManagedTx(inManagedTx);
                    for (ManagedConnectionProxy proxy:proxyConnections) {
                        proxy.setUseSharedTxContext(inManagedTx);
                    }
                }
            };

            this.transactionContext.setLocalTransactionEventListener(new LocalTransactionEventListener() {
                public void beginEvent() {
                    fireBeginEvent();
                }

                public void commitEvent() {
                    fireCommitEvent();
                }

                public void rollbackEvent() {
                    fireRollbackEvent();
                }
            });

            physicalConnection.setExceptionListener(this);
        } catch (JMSException e) {
            throw new ResourceException("Could not create a new connection: " + e.getMessage(), e);
        }
    }

    public boolean isInManagedTx() {
        return localAndXATransaction.isInManagedTx();
    }

    public static boolean matches(Object x, Object y) {
        if (x == null ^ y == null) {
            return false;
        }
        if (x != null && !x.equals(y)) {
            return false;
        }
        return true;
    }

    public void associate(Subject subject, ActiveMQConnectionRequestInfo info) throws JMSException {

        // Do we need to change the associated userid/password
        if (!matches(info.getUserName(), this.info.getUserName()) || !matches(info.getPassword(), this.info.getPassword())) {
            physicalConnection.changeUserInfo(info.getUserName(), info.getPassword());
        }

        // Do we need to set the clientId?
        if (info.getClientid() != null && info.getClientid().length() > 0) {
            physicalConnection.setClientID(info.getClientid());
        }

        this.subject = subject;
        this.info = info;
    }

    public Connection getPhysicalConnection() {
        return physicalConnection;
    }

    private void fireBeginEvent() {
        ConnectionEvent event = new ConnectionEvent(ActiveMQManagedConnection.this, ConnectionEvent.LOCAL_TRANSACTION_STARTED);
        for(ConnectionEventListener l:listeners) {
            l.localTransactionStarted(event);
        }
    }

    private void fireCommitEvent() {
        ConnectionEvent event = new ConnectionEvent(ActiveMQManagedConnection.this, ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
        for(ConnectionEventListener l:listeners) {
            l.localTransactionCommitted(event);
        }
    }

    private void fireRollbackEvent() {
        ConnectionEvent event = new ConnectionEvent(ActiveMQManagedConnection.this, ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
        for(ConnectionEventListener l:listeners) {
            l.localTransactionRolledback(event);
        }
    }

    private void fireCloseEvent(ManagedConnectionProxy proxy) {
        ConnectionEvent event = new ConnectionEvent(ActiveMQManagedConnection.this, ConnectionEvent.CONNECTION_CLOSED);
        event.setConnectionHandle(proxy);

        for(ConnectionEventListener l:listeners) {
            l.connectionClosed(event);
        }
    }

    private void fireErrorOccurredEvent(Exception error) {
        ConnectionEvent event = new ConnectionEvent(ActiveMQManagedConnection.this, ConnectionEvent.CONNECTION_ERROR_OCCURRED, error);
        for(ConnectionEventListener l:listeners) {
            l.connectionErrorOccurred(event);
        }
    }

    /**
     * @see javax.resource.spi.ManagedConnection#getConnection(javax.security.auth.Subject,
     *      javax.resource.spi.ConnectionRequestInfo)
     */
    public Object getConnection(Subject subject, ConnectionRequestInfo info) throws ResourceException {
        ManagedConnectionProxy proxy = new ManagedConnectionProxy(this);
        proxyConnections.add(proxy);
        return proxy;
    }

    private boolean isDestroyed() {
        return destroyed;
    }

    /**
     * Close down the physical connection to the server.
     * 
     * @see javax.resource.spi.ManagedConnection#destroy()
     */
    public void destroy() throws ResourceException {
        // Have we allready been destroyed??
        if (isDestroyed()) {
            return;
        }

        cleanup();

        try {
            physicalConnection.close();
            destroyed = true;
        } catch (JMSException e) {
            LOG.info("Error occured during close of a JMS connection.", e);
        }
    }

    /**
     * Cleans up all proxy handles attached to this physical connection so that
     * they cannot be used anymore.
     * 
     * @see javax.resource.spi.ManagedConnection#cleanup()
     */
    public void cleanup() throws ResourceException {

        // Have we allready been destroyed??
        if (isDestroyed()) {
            return;
        }

        for (ManagedConnectionProxy proxy:proxyConnections) {
            proxy.cleanup();
        }
        proxyConnections.clear();

        try {
            physicalConnection.cleanup();
        } catch (JMSException e) {
            throw new ResourceException("Could cleanup the ActiveMQ connection: " + e, e);
        }
        // defer transaction cleanup till after close so that close is aware of the current tx
        localAndXATransaction.cleanup();

    }

    /**
     * @see javax.resource.spi.ManagedConnection#associateConnection(java.lang.Object)
     */
    public void associateConnection(Object connection) throws ResourceException {
        if (connection instanceof ManagedConnectionProxy) {
            ManagedConnectionProxy proxy = (ManagedConnectionProxy)connection;
            proxyConnections.add(proxy);
        } else {
            throw new ResourceException("Not supported : associating connection instance of " + connection.getClass().getName());
        }
    }

    /**
     * @see javax.resource.spi.ManagedConnection#addConnectionEventListener(javax.resource.spi.ConnectionEventListener)
     */
    public void addConnectionEventListener(ConnectionEventListener listener) {
        listeners.add(listener);
    }

    /**
     * @see javax.resource.spi.ManagedConnection#removeConnectionEventListener(javax.resource.spi.ConnectionEventListener)
     */
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        listeners.remove(listener);
    }

    /**
     * @see javax.resource.spi.ManagedConnection#getXAResource()
     */
    public XAResource getXAResource() throws ResourceException {
        return localAndXATransaction;
    }

    /**
     * @see javax.resource.spi.ManagedConnection#getLocalTransaction()
     */
    public LocalTransaction getLocalTransaction() throws ResourceException {
        return localAndXATransaction;
    }

    /**
     * @see javax.resource.spi.ManagedConnection#getMetaData()
     */
    public ManagedConnectionMetaData getMetaData() throws ResourceException {
        return new ManagedConnectionMetaData() {

            public String getEISProductName() throws ResourceException {
                if (physicalConnection == null) {
                    throw new ResourceException("Not connected.");
                }
                try {
                    return physicalConnection.getMetaData().getJMSProviderName();
                } catch (JMSException e) {
                    throw new ResourceException("Error accessing provider.", e);
                }
            }

            public String getEISProductVersion() throws ResourceException {
                if (physicalConnection == null) {
                    throw new ResourceException("Not connected.");
                }
                try {
                    return physicalConnection.getMetaData().getProviderVersion();
                } catch (JMSException e) {
                    throw new ResourceException("Error accessing provider.", e);
                }
            }

            public int getMaxConnections() throws ResourceException {
                if (physicalConnection == null) {
                    throw new ResourceException("Not connected.");
                }
                return Integer.MAX_VALUE;
            }

            public String getUserName() throws ResourceException {
                if (physicalConnection == null) {
                    throw new ResourceException("Not connected.");
                }
                try {
                    return physicalConnection.getClientID();
                } catch (JMSException e) {
                    throw new ResourceException("Error accessing provider.", e);
                }
            }
        };
    }

    /**
     * @see javax.resource.spi.ManagedConnection#setLogWriter(java.io.PrintWriter)
     */
    public void setLogWriter(PrintWriter logWriter) throws ResourceException {
        this.logWriter = logWriter;
    }

    /**
     * @see javax.resource.spi.ManagedConnection#getLogWriter()
     */
    public PrintWriter getLogWriter() throws ResourceException {
        return logWriter;
    }

    /**
     * @param subject subject to match
     * @param info cri to match
     * @return whether the subject and cri match sufficiently to allow using this connection under the new circumstances
     */
    public boolean matches(Subject subject, ConnectionRequestInfo info) {
        // Check to see if it is our info class
        if (info == null) {
            return false;
        }
        if (info.getClass() != ActiveMQConnectionRequestInfo.class) {
            return false;
        }

        // Do the subjects match?
        if (subject == null ^ this.subject == null) {
            return false;
        }
        if (subject != null && !subject.equals(this.subject)) {
            return false;
        }

        // Does the info match?
        return info.equals(this.info);
    }

    /**
     * When a proxy is closed this cleans up the proxy and notifys the
     * ConnectionEventListeners that a connection closed.
     * 
     * @param proxy
     */
    public void proxyClosedEvent(ManagedConnectionProxy proxy) {
        proxyConnections.remove(proxy);
        proxy.cleanup();
        fireCloseEvent(proxy);
    }

    public void onException(JMSException e) {
        LOG.warn("Connection failed: " + e);
        LOG.debug("Cause: ", e);

        for (ManagedConnectionProxy proxy:proxyConnections) {
            proxy.onException(e);
        }
        // Let the container know that the error occured.
        fireErrorOccurredEvent(e);
    }

    /**
     * @return Returns the transactionContext.
     */
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

}
