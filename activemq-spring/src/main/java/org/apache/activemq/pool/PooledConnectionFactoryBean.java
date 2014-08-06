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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.ConnectionFactory;
import javax.transaction.TransactionManager;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

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
 * The <code>resourceName</code> property should be used along with the {@link org.apache.activemq.jms.pool.GenericResourceManager} and have
 * the same value than its <code>resourceName</code> property. This will make sure the transaction manager
 * maps correctly the connection factory to the recovery process.
 *
 * @org.apache.xbean.XBean
 */
public class PooledConnectionFactoryBean implements FactoryBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(PooledConnectionFactoryBean.class);

    private PooledConnectionFactory pooledConnectionFactory;
    private ConnectionFactory connectionFactory;
    private int maxConnections = 1;
    private int maximumActive = 500;
    private Object transactionManager;
    private String resourceName;

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

    /**
     * JSR-250 callback wrapper; converts checked exceptions to runtime exceptions
     *
     * delegates to afterPropertiesSet, done to prevent backwards incompatible signature change.
     */
    @PostConstruct
    private void postConstruct() {
        try {
            afterPropertiesSet();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *
     * @throws Exception
     * @org.apache.xbean.InitMethod
     */
    public void afterPropertiesSet() throws Exception {
        if (pooledConnectionFactory == null && transactionManager != null && resourceName != null) {
            try {
                LOGGER.debug("Trying to build a JcaPooledConnectionFactory");
                JcaPooledConnectionFactory f = new JcaPooledConnectionFactory();
                f.setName(resourceName);
                f.setTransactionManager((TransactionManager) transactionManager);
                f.setMaxConnections(maxConnections);
                f.setMaximumActiveSessionPerConnection(maximumActive);
                f.setConnectionFactory(connectionFactory);
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
                f.setMaximumActiveSessionPerConnection(maximumActive);
                f.setConnectionFactory(connectionFactory);
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
                f.setMaximumActiveSessionPerConnection(maximumActive);
                f.setConnectionFactory(connectionFactory);
                this.pooledConnectionFactory = f;
            } catch (Throwable t) {
                LOGGER.debug("Could not create pooled connection factory: " + t, t);
            }
        }
        if (pooledConnectionFactory == null) {
            throw new IllegalStateException("Unable to create pooled connection factory.  Enable DEBUG log level for more informations");
        }
    }

    /**
     * JSR-250 callback wrapper; converts checked exceptions to runtime exceptions
     *
     * delegates to destroy, done to prevent backwards incompatible signature change.
     */
    @PreDestroy
    private void preDestroy() {
        try {
            destroy();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *
     * @throws Exception
     * @org.apache.xbean.DestroyMethod
     */
    public void destroy() throws Exception {
        if (pooledConnectionFactory != null) {
            pooledConnectionFactory.stop();
            pooledConnectionFactory = null;
        }
    }

    @Override
    public Object getObject() throws Exception {
        // in case spring-dm calls getObject before this bean has been initialized
        if (pooledConnectionFactory == null) {
            afterPropertiesSet();
        }
        return pooledConnectionFactory;
    }

    @Override
    public Class getObjectType() {
        return ConnectionFactory.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
