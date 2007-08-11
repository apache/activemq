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

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * A simple implementation of a ConnectionManager that can be extended so that it can
 * see how the RA connections are interacting with it.
 *  
 * @version $Revision$
 */
public class ConnectionManagerAdapter implements ConnectionManager, ConnectionEventListener {
    
    private static final long serialVersionUID = 5205646563916645831L;
    
    private static final Log log = LogFactory.getLog(ConnectionManagerAdapter.class);
    ArrayList listners = new ArrayList();
    ArrayList connections = new ArrayList();
    
    /**
     * Adds a listner to all connections created by this connection manager.
     * This listner will be added to all previously created connections. 
     * 
     * @param l
     */
    public void addConnectionEventListener(ConnectionEventListener l ) {
        for (Iterator iter = connections.iterator(); iter.hasNext();) {
            ManagedConnection c = (ManagedConnection) iter.next();
            c.addConnectionEventListener(l);
        }
        listners.add(l);
    }
    
    /**
     * @see javax.resource.spi.ConnectionManager#allocateConnection(javax.resource.spi.ManagedConnectionFactory, javax.resource.spi.ConnectionRequestInfo)
     */
    public Object allocateConnection(ManagedConnectionFactory connectionFactory, ConnectionRequestInfo info) throws ResourceException {
        Subject subject = null;
        ManagedConnection connection = connectionFactory.createManagedConnection(subject, info);
        connection.addConnectionEventListener(this);
        for (Iterator iter = listners.iterator(); iter.hasNext();) {
            ConnectionEventListener l = (ConnectionEventListener) iter.next();
            connection.addConnectionEventListener(l);
        }
        connections.add(connection);
        return connection.getConnection(subject, info);
    }

    /**
     * @see javax.resource.spi.ConnectionEventListener#connectionClosed(javax.resource.spi.ConnectionEvent)
     */
    public void connectionClosed(ConnectionEvent event) {
        connections.remove(event.getSource());
        try {
            ((ManagedConnection)event.getSource()).cleanup();
        } catch (ResourceException e) {
            log.warn("Error occured during the cleanup of a managed connection: ",e);
        }
        try {
            ((ManagedConnection)event.getSource()).destroy();
        } catch (ResourceException e) {
            log.warn("Error occured during the destruction of a managed connection: ",e);
        }
    }

    /**
     * @see javax.resource.spi.ConnectionEventListener#localTransactionStarted(javax.resource.spi.ConnectionEvent)
     */
    public void localTransactionStarted(ConnectionEvent event) {
    }

    /**
     * @see javax.resource.spi.ConnectionEventListener#localTransactionCommitted(javax.resource.spi.ConnectionEvent)
     */
    public void localTransactionCommitted(ConnectionEvent event) {
    }

    /**
     * @see javax.resource.spi.ConnectionEventListener#localTransactionRolledback(javax.resource.spi.ConnectionEvent)
     */
    public void localTransactionRolledback(ConnectionEvent event) {
    }

    /**
     * @see javax.resource.spi.ConnectionEventListener#connectionErrorOccurred(javax.resource.spi.ConnectionEvent)
     */
    public void connectionErrorOccurred(ConnectionEvent event) {
        log.warn("Managed connection experiened an error: ",event.getException());
        try {
            ((ManagedConnection)event.getSource()).cleanup();
        } catch (ResourceException e) {
            log.warn("Error occured during the cleanup of a managed connection: ",e);
        }
        try {
            ((ManagedConnection)event.getSource()).destroy();
        } catch (ResourceException e) {
            log.warn("Error occured during the destruction of a managed connection: ",e);
        }
    }

}
