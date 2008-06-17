/*
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

import javax.jms.ConnectionFactory;
import javax.transaction.TransactionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.ObjectPoolFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Simple factory bean used to create a jencks connection pool.
 * Depending on the properties set, it will create a simple pool,
 * a transaction aware connection pool, or a jca aware connection pool.
 *
 * <pre class="code">
 * <bean id="pooledConnectionFactory" class="javax.script.ScriptEngineFactory.PooledConnectionFactoryFactoryBean">
 *   <property name="connectionFactory" ref="connectionFactory" />
 *   <property name="transactionManager" ref="transactionManager" />
 *   <property name="resourceName" value="ResourceName" />
 * </bean>
 * </pre>
 *
 * The <code>resourceName</code> property should be used along with the {@link ActiveMQResourceManager} and have
 * the same value than its <code>resourceName</code> property. This will make sure the transaction manager
 * maps correctly the connection factory to the recovery process.
 *
 */
public class PooledConnectionFactoryBean implements FactoryBean, InitializingBean {

    private static final Log LOGGER = LogFactory.getLog(PooledConnectionFactoryBean.class);

    private ConnectionFactory pooledConnectionFactory;
    private ConnectionFactory connectionFactory;
    private int maxConnections;
    private int maximumActive;
    private Object transactionManager;
    private String resourceName;
    private ObjectPoolFactory poolFactory;

    public Object getObject() throws Exception {
        return pooledConnectionFactory;
    }

    public Class getObjectType() {
        return ConnectionFactory.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getMaximumActive() {
        return maximumActive;
    }

    public void setMaximumActive(int maximumActive) {
        this.maximumActive = maximumActive;
    }

    public Object getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(Object transactionManager) {
        this.transactionManager = transactionManager;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public ObjectPoolFactory getPoolFactory() {
        return poolFactory;
    }

    public void setPoolFactory(ObjectPoolFactory poolFactory) {
        this.poolFactory = poolFactory;
    }

    public void afterPropertiesSet() throws Exception {
        if (pooledConnectionFactory == null && transactionManager != null && resourceName != null) {
            try {
                LOGGER.debug("Trying to build a JcaPooledConnectionFactory");
                JcaPooledConnectionFactory f = new JcaPooledConnectionFactory();
                f.setName(resourceName);
                f.setTransactionManager((TransactionManager) transactionManager);
                f.setMaxConnections(maxConnections);
                f.setMaximumActive(maximumActive);
                f.setConnectionFactory(connectionFactory);
                f.setPoolFactory(poolFactory);
                this.pooledConnectionFactory = f;
            } catch (Throwable t) {
                LOGGER.debug("Could not create JCA enabled connection factory: " + t, t);
            }
        }
        if (pooledConnectionFactory == null && transactionManager != null) {
            try {
                LOGGER.debug("Trying to build a XaPooledConnectionFactory");
                XaPooledConnectionFactory f = new XaPooledConnectionFactory();
                f.setTransactionManager((TransactionManager) transactionManager);
                f.setMaxConnections(maxConnections);
                f.setMaximumActive(maximumActive);
                f.setConnectionFactory(connectionFactory);
                f.setPoolFactory(poolFactory);
                this.pooledConnectionFactory = f;
            } catch (Throwable t) {
                LOGGER.debug("Could not create XA enabled connection factory: " + t, t);
            }
        }
        if (pooledConnectionFactory == null) {
            try {
                LOGGER.debug("Trying to build a PooledConnectionFactory");
                PooledConnectionFactory f = new PooledConnectionFactory();
                f.setMaxConnections(maxConnections);
                f.setMaximumActive(maximumActive);
                f.setConnectionFactory(connectionFactory);
                f.setPoolFactory(poolFactory);
                this.pooledConnectionFactory = f;
            } catch (Throwable t) {
                LOGGER.debug("Could not create pooled connection factory: " + t, t);
            }
        }
        if (pooledConnectionFactory == null) {
            throw new IllegalStateException("Unable to create pooled connection factory.  Enable DEBUG log level for more informations");
        }
    }
}
