/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Set;

import javax.jms.JMSException;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.security.auth.Subject;

/**
 * @version $Revisio    n$
 * 
 * TODO: Must override equals and hashCode (JCA spec 16.4) 
 */
public class ActiveMQManagedConnectionFactory implements
        ManagedConnectionFactory, ResourceAdapterAssociation {

    private static final long serialVersionUID = 6196921962230582875L;
    
    private ActiveMQResourceAdapter adapter;
    private PrintWriter logWriter;
    private ActiveMQConnectionRequestInfo info = new ActiveMQConnectionRequestInfo();
    
    public void setResourceAdapter(ResourceAdapter adapter) throws ResourceException {
        this.adapter = (ActiveMQResourceAdapter) adapter;
        ActiveMQConnectionRequestInfo baseInfo = this.adapter.getInfo().copy();
        if( info.getClientid()==null ) 
            info.setClientid(baseInfo.getClientid());
        if( info.getPassword()==null )
            info.setPassword(baseInfo.getPassword());
        if( info.getServerUrl()==null )
            info.setServerUrl(baseInfo.getServerUrl());
        if( info.getUseInboundSession()==null )
            info.setUseInboundSession(baseInfo.getUseInboundSession());
        if( info.getUserName()==null )
            info.setUserName(baseInfo.getUserName());
    }

    public ResourceAdapter getResourceAdapter() {
        return adapter;
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#createConnectionFactory(javax.resource.spi.ConnectionManager)
     */
    public Object createConnectionFactory(ConnectionManager manager) throws ResourceException {
        return new ActiveMQConnectionFactory(this, manager, info);
    }

    /**
     * This is used when not running in an app server.  For now we are creating a
     * ConnectionFactory that has our SimpleConnectionManager implementation but
     * it may be a better idea to not support this.  The JMS api will have many quirks
     * the user may not expect when running through the resource adapter.
     *
     * @see javax.resource.spi.ManagedConnectionFactory#createConnectionFactory()
     */
    public Object createConnectionFactory() throws ResourceException {
        return new ActiveMQConnectionFactory(this, new SimpleConnectionManager(), info);
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#createManagedConnection(javax.security.auth.Subject,
     *      javax.resource.spi.ConnectionRequestInfo)
     */
    public ManagedConnection createManagedConnection(Subject subject, ConnectionRequestInfo info) throws ResourceException {
        try {
            ActiveMQConnectionRequestInfo amqInfo = (ActiveMQConnectionRequestInfo)info;
            return new ActiveMQManagedConnection(subject, adapter.makeConnection(amqInfo), amqInfo);
        } catch (JMSException e) {
            throw new ResourceException("Could not create connection.", e);
        }
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#matchManagedConnections(java.util.Set,
            *      javax.security.auth.Subject,
            *      javax.resource.spi.ConnectionRequestInfo)
     */
    public ManagedConnection matchManagedConnections(Set connections, Subject subject, ConnectionRequestInfo info) throws ResourceException {
        Iterator iterator = connections.iterator();
        while (iterator.hasNext()) {
            ActiveMQManagedConnection c = (ActiveMQManagedConnection) iterator.next();
            if (c.matches(subject, info)) {
                try {
                    c.associate(subject, (ActiveMQConnectionRequestInfo) info);
                    return c;
                } catch (JMSException e) {
                    throw new ResourceException(e);
                }
            }
        }
        return null;
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#setLogWriter(java.io.PrintWriter)
     */
    public void setLogWriter(PrintWriter logWriter) throws ResourceException {
        this.logWriter = logWriter;
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#getLogWriter()
     */
    public PrintWriter getLogWriter() throws ResourceException {
        return logWriter;
    }

    ///////////////////////////////////////////////////////////////////////////
    //
    // Bean setters and getters.
    //
    ///////////////////////////////////////////////////////////////////////////
    
    public String getClientid() {
        return info.getClientid();
    }

    public String getPassword() {
        return info.getPassword();
    }

    public String getServerUrl() {
        return info.getServerUrl();
    }

    public String getUserName() {
        return info.getUserName();
    }

    public void setClientid(String clientid) {
        info.setClientid(clientid);
    }

    public void setPassword(String password) {
        info.setPassword(password);
    }

    public void setServerUrl(String url) {
        info.setServerUrl(url);
    }

    public void setUserName(String userid) {
        info.setUserName(userid);
    }

    public Boolean getUseInboundSession() {
        return info.getUseInboundSession();
    }

    public void setUseInboundSession(Boolean useInboundSession) {
        info.setUseInboundSession(useInboundSession);
    }
}
