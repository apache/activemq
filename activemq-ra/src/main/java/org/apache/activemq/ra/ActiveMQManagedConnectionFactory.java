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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
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

import org.slf4j.LoggerFactory;

/**
 * @version $Revisio n$ TODO: Must override equals and hashCode (JCA spec 16.4)
 * @org.apache.xbean.XBean element="managedConnectionFactory"
 */
public class ActiveMQManagedConnectionFactory extends ActiveMQConnectionSupport implements ManagedConnectionFactory, ResourceAdapterAssociation {

    private static final long serialVersionUID = 6196921962230582875L;
    private PrintWriter logWriter;

    /**
     * @see javax.resource.spi.ResourceAdapterAssociation#setResourceAdapter(javax.resource.spi.ResourceAdapter)
     */
    @Override
    public void setResourceAdapter(ResourceAdapter adapter) throws ResourceException {
        if (!(adapter instanceof MessageResourceAdapter)) {
            throw new ResourceException("ResourceAdapter is not of type: " + MessageResourceAdapter.class.getName());
        } else {
            if (log.isDebugEnabled()) {
                log.debug(this + ", copying standard ResourceAdapter configuration properties");
            }

            ActiveMQConnectionRequestInfo baseInfo = ((MessageResourceAdapter) adapter).getInfo().copy();
            if (getClientid() == null) {
                setClientid(baseInfo.getClientid());
            }
            if (getPassword() == null) {
                setPassword(baseInfo.getPassword());
            }
            if (getServerUrl() == null) {
                setServerUrl(baseInfo.getServerUrl());
            }
            if (getUseInboundSession() == null) {
                setUseInboundSession(baseInfo.getUseInboundSession());
            }
            if (getUseSessionArgs() == null) {
                setUseSessionArgs(baseInfo.isUseSessionArgs());
            }
            if (getUserName() == null) {
                setUserName(baseInfo.getUserName());
            }
            if (getDurableTopicPrefetch() != null) {
                setDurableTopicPrefetch(baseInfo.getDurableTopicPrefetch());
            }
            if (getOptimizeDurableTopicPrefetch() != null) {
                setOptimizeDurableTopicPrefetch(baseInfo.getOptimizeDurableTopicPrefetch());
            }
            if (getQueuePrefetch() != null) {
                setQueuePrefetch(baseInfo.getQueuePrefetch());
            }
            if (getQueueBrowserPrefetch() != null) {
                setQueueBrowserPrefetch(baseInfo.getQueueBrowserPrefetch());
            }
            if (getTopicPrefetch() != null) {
                setTopicPrefetch(baseInfo.getTopicPrefetch());
            }
        }
    }

    /**
     * @see javax.resource.spi.ResourceAdapterAssociation#getResourceAdapter()
     */
    @Override
    public ResourceAdapter getResourceAdapter() {
        return null;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object object) {
        if (object == null || object.getClass() != ActiveMQManagedConnectionFactory.class) {
            return false;
        }
        return ((ActiveMQManagedConnectionFactory) object).getInfo().equals(getInfo());
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return getInfo().hashCode();
    }

    /**
     * Writes this factory during serialization along with the superclass'
     * <i>info</i> property. This needs to be done manually since the superclass
     * is not serializable itself.
     *
     * @param out
     *        the stream to write object state to
     * @throws java.io.IOException
     *         if the object cannot be serialized
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        if (logWriter != null && !(logWriter instanceof Serializable)) {
            // if the PrintWriter injected by the application server is not
            // serializable we just drop the reference and let the application
            // server re-inject a PrintWriter later (after this factory has been
            // deserialized again) using the standard setLogWriter() method
            logWriter = null;
        }
        out.defaultWriteObject();
        out.writeObject(getInfo());
    }

    /**
     * Restores this factory along with the superclass' <i>info</i> property.
     * This needs to be done manually since the superclass is not serializable
     * itself.
     *
     * @param in
     *        the stream to read object state from
     * @throws java.io.IOException
     *         if the object state could not be restored
     * @throws java.lang.ClassNotFoundException
     *         if the object state could not be restored
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        setInfo((ActiveMQConnectionRequestInfo) in.readObject());
        log = LoggerFactory.getLogger(getClass());
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#createConnectionFactory(javax.resource.spi.ConnectionManager)
     */
    @Override
    public Object createConnectionFactory(ConnectionManager manager) throws ResourceException {
        return new ActiveMQConnectionFactory(this, manager, getInfo());
    }

    /**
     * This is used when not running in an app server. For now we are creating a
     * ConnectionFactory that has our SimpleConnectionManager implementation but
     * it may be a better idea to not support this. The JMS api will have many
     * quirks the user may not expect when running through the resource adapter.
     *
     * @see javax.resource.spi.ManagedConnectionFactory#createConnectionFactory()
     */
    @Override
    public Object createConnectionFactory() throws ResourceException {
        return new ActiveMQConnectionFactory(this, new SimpleConnectionManager(), getInfo());
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#createManagedConnection(javax.security.auth.Subject,
     *      javax.resource.spi.ConnectionRequestInfo)
     */
    @Override
    public ManagedConnection createManagedConnection(Subject subject, ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
        ActiveMQConnectionRequestInfo amqInfo = getInfo();
        if (connectionRequestInfo instanceof ActiveMQConnectionRequestInfo) {
            amqInfo = (ActiveMQConnectionRequestInfo) connectionRequestInfo;
        }
        try {
            return new ActiveMQManagedConnection(subject, makeConnection(amqInfo), amqInfo);
        } catch (JMSException e) {
            throw new ResourceException("Could not create connection.", e);
        }
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#matchManagedConnections(java.util.Set,
     *      javax.security.auth.Subject,
     *      javax.resource.spi.ConnectionRequestInfo)
     */
    @Override
    public ManagedConnection matchManagedConnections(Set connections, Subject subject, ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
        Iterator iterator = connections.iterator();
        while (iterator.hasNext()) {
            ActiveMQManagedConnection c = (ActiveMQManagedConnection) iterator.next();
            if (c.matches(subject, connectionRequestInfo)) {
                try {
                    c.associate(subject, (ActiveMQConnectionRequestInfo) connectionRequestInfo);
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
    @Override
    public void setLogWriter(PrintWriter aLogWriter) throws ResourceException {
        if (log.isTraceEnabled()) {
            log.trace("setting log writer [" + aLogWriter + "]");
        }
        this.logWriter = aLogWriter;
    }

    /**
     * @see javax.resource.spi.ManagedConnectionFactory#getLogWriter()
     */
    @Override
    public PrintWriter getLogWriter() throws ResourceException {
        if (log.isTraceEnabled()) {
            log.trace("getting log writer [" + logWriter + "]");
        }
        return logWriter;
    }
}
