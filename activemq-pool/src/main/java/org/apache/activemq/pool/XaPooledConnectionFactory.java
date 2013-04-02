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

import java.io.Serializable;
import java.util.Hashtable;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NamingEnumeration;
import javax.naming.spi.ObjectFactory;
import javax.transaction.TransactionManager;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pooled connection factory that automatically enlists
 * sessions in the current active XA transaction if any.
 */
public class XaPooledConnectionFactory extends PooledConnectionFactory implements ObjectFactory,
        Serializable, QueueConnectionFactory, TopicConnectionFactory {

    private static final transient Logger LOG = LoggerFactory.getLogger(XaPooledConnectionFactory.class);
    private TransactionManager transactionManager;
    private boolean tmFromJndi = false;
    private String tmJndiName = "java:/TransactionManager";
    private String brokerUrl = null;

    public XaPooledConnectionFactory() {
        super();
    }

    public XaPooledConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    public XaPooledConnectionFactory(String brokerURL) {
        super(brokerURL);
    }

    public TransactionManager getTransactionManager() {
        if (transactionManager == null && tmFromJndi) {
            try {
                transactionManager = (TransactionManager) new InitialContext().lookup(getTmJndiName());
            } catch (Throwable ignored) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("exception on tmFromJndi: " + getTmJndiName(), ignored);
                }
            }
        }
        return transactionManager;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    protected ConnectionPool createConnectionPool(ActiveMQConnection connection) {
        return new XaConnectionPool(connection, getTransactionManager());
    }

    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {
        setTmFromJndi(true);
        configFromJndiConf(obj);
        if (environment != null) {
            IntrospectionSupport.setProperties(this, environment);
        }
        return this;
    }

    private void configFromJndiConf(Object rootContextName) {
        if (rootContextName instanceof String) {
            String name = (String) rootContextName;
            name = name.substring(0, name.lastIndexOf('/')) + "/conf" + name.substring(name.lastIndexOf('/'));
            try {
                InitialContext ctx = new InitialContext();
                NamingEnumeration bindings = ctx.listBindings(name);

                while (bindings.hasMore()) {
                    Binding bd = (Binding)bindings.next();
                    IntrospectionSupport.setProperty(this, bd.getName(), bd.getObject());
                }

            } catch (Exception ignored) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("exception on config from jndi: " + name, ignored);
                }
            }
        }
    }

    public void setBrokerUrl(String url) {
        if (brokerUrl == null || !brokerUrl.equals(url)) {
            brokerUrl = url;
            setConnectionFactory(new ActiveMQConnectionFactory(brokerUrl));
        }
    }

    public String getTmJndiName() {
        return tmJndiName;
    }

    public void setTmJndiName(String tmJndiName) {
        this.tmJndiName = tmJndiName;
    }

    public boolean isTmFromJndi() {
        return tmFromJndi;
    }

    /**
     * Allow transaction manager resolution from JNDI (ee deployment)
     * @param tmFromJndi
     */
    public void setTmFromJndi(boolean tmFromJndi) {
        this.tmFromJndi = tmFromJndi;
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection) createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection) createConnection(userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return (TopicConnection) createConnection();
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return (TopicConnection) createConnection(userName, password);
    }
}
