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
package org.apache.activemq.jms.pool;

import java.io.Serializable;
import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NamingEnumeration;
import javax.naming.spi.ObjectFactory;
import javax.transaction.TransactionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pooled connection factory that automatically enlists sessions in the
 * current active XA transaction if any.
 */
public class XaPooledConnectionFactory extends PooledConnectionFactory implements ObjectFactory, Serializable {

    private static final transient Logger LOG = LoggerFactory.getLogger(XaPooledConnectionFactory.class);
    private static final long serialVersionUID = -6545688026350913005L;

    private TransactionManager transactionManager;
    private boolean tmFromJndi = false;
    private String tmJndiName = "java:/TransactionManager";

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
    public void setConnectionFactory(Object toUse) {
        if (toUse instanceof XAConnectionFactory) {
            connectionFactory = toUse;
        } else {
            throw new IllegalArgumentException("connectionFactory should implement javax.xml.XAConnectionFactory");
        }
    }

    @Override
    protected Connection createConnection(ConnectionKey key) throws JMSException {
        if (connectionFactory instanceof XAConnectionFactory) {
            if (key.getUserName() == null && key.getPassword() == null) {
                return ((XAConnectionFactory) connectionFactory).createXAConnection();
            } else {
                return ((XAConnectionFactory) connectionFactory).createXAConnection(key.getUserName(), key.getPassword());
            }
        } else {
            throw new IllegalStateException("connectionFactory should implement javax.jms.XAConnectionFactory");
        }
    }

    @Override
    protected ConnectionPool createConnectionPool(Connection connection) {
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
                NamingEnumeration<Binding> bindings = ctx.listBindings(name);

                while (bindings.hasMore()) {
                    Binding bd = bindings.next();
                    IntrospectionSupport.setProperty(this, bd.getName(), bd.getObject());
                }

            } catch (Exception ignored) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("exception on config from jndi: " + name, ignored);
                }
            }
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
     *
     * @param tmFromJndi
     */
    public void setTmFromJndi(boolean tmFromJndi) {
        this.tmFromJndi = tmFromJndi;
    }
}
