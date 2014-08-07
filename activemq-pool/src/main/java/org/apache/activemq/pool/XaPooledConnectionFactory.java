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
package org.apache.activemq.pool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.transaction.xa.XAResource;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.jms.pool.PooledSession;
import org.apache.activemq.jms.pool.SessionKey;
import org.apache.activemq.jms.pool.XaConnectionPool;
import org.apache.activemq.jndi.JNDIReferenceFactory;
import org.apache.activemq.jndi.JNDIStorableInterface;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IntrospectionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  * Add Service and Referenceable and TransportListener to @link{org.apache.activemq.jms.pool.XaPooledConnectionFactory}
  *
  * @org.apache.xbean.XBean element=xaPooledConnectionFactory"
  */
public class XaPooledConnectionFactory extends org.apache.activemq.jms.pool.XaPooledConnectionFactory implements JNDIStorableInterface, Service {
    public static final String POOL_PROPS_PREFIX = "pool";
    private static final transient Logger LOG = LoggerFactory.getLogger(org.apache.activemq.jms.pool.XaPooledConnectionFactory.class);
    private String brokerUrl;

    public XaPooledConnectionFactory() {
        super();
    }

    public XaPooledConnectionFactory(ActiveMQXAConnectionFactory connectionFactory) {
        setConnectionFactory(connectionFactory);
    }

    @Override
    protected org.apache.activemq.jms.pool.ConnectionPool createConnectionPool(Connection connection) {
        return new XaConnectionPool(connection, getTransactionManager()) {

            @Override
            protected Session makeSession(SessionKey key) throws JMSException {
                if (connection instanceof XAConnection) {
                    return ((XAConnection)connection).createXASession();
                } else {
                    return connection.createSession(key.isTransacted(), key.getAckMode());
                }
            }

            @Override
            protected XAResource createXaResource(PooledSession session) throws JMSException {
                if (session.getInternalSession() instanceof XASession) {
                    return ((XASession)session.getInternalSession()).getXAResource();
                } else {
                    return ((ActiveMQSession)session.getInternalSession()).getTransactionContext();
                }
            }


            @Override
            protected Connection wrap(final Connection connection) {
                // Add a transport Listener so that we can notice if this connection
                // should be expired due to a connection failure.
                ((ActiveMQConnection)connection).addTransportListener(new TransportListener() {
                    @Override
                    public void onCommand(Object command) {
                    }

                    @Override
                    public void onException(IOException error) {
                        synchronized (this) {
                            setHasExpired(true);
                            // only log if not stopped
                            if (!stopped.get()) {
                                LOG.info("Expiring connection " + connection + " on IOException: " + error.getMessage());
                                // log stacktrace at debug level
                                LOG.debug("Expiring connection " + connection + " on IOException: ", error);
                            }
                        }
                    }

                    @Override
                    public void transportInterupted() {
                    }

                    @Override
                    public void transportResumed() {
                    }
                });

                // make sure that we set the hasFailed flag, in case the transport already failed
                // prior to the addition of our new TransportListener
                setHasExpired(((ActiveMQConnection) connection).isTransportFailed());

                // may want to return an amq EnhancedConnection
                return connection;
            }

            @Override
            protected void unWrap(Connection connection) {
                if (connection != null) {
                    ((ActiveMQConnection)connection).cleanUpTempDestinations();
                }
            }
        };
    }

    protected void buildFromProperties(Properties props) {
        ActiveMQConnectionFactory activeMQConnectionFactory = props.containsKey("xaAckMode") ?
                new ActiveMQXAConnectionFactory() : new ActiveMQConnectionFactory();
        activeMQConnectionFactory.buildFromProperties(props);
        setConnectionFactory(activeMQConnectionFactory);
        IntrospectionSupport.setProperties(this, new HashMap(props), POOL_PROPS_PREFIX);
    }

    protected void populateProperties(Properties props) {
        ((ActiveMQConnectionFactory)getConnectionFactory()).populateProperties(props);
        IntrospectionSupport.getProperties(this, props, POOL_PROPS_PREFIX);
    }

    @Override
    public void setProperties(Properties properties) {
        buildFromProperties(properties);
    }

    @Override
    public Properties getProperties() {
        Properties properties = new Properties();
        populateProperties(properties);
        return properties;
    }

    @Override
    public Reference getReference() throws NamingException {
        return JNDIReferenceFactory.createReference(this.getClass().getName(), this);
    }

    public void setBrokerUrl(String url) {
        if (brokerUrl == null || !brokerUrl.equals(url)) {
            brokerUrl = url;
            setConnectionFactory(new ActiveMQXAConnectionFactory(brokerUrl));
        }
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }
}
