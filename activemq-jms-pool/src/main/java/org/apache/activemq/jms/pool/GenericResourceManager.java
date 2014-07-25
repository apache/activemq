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
package org.apache.activemq.jms.pool;

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import javax.transaction.xa.XAResource;
import org.apache.geronimo.transaction.manager.NamedXAResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.geronimo.transaction.manager.RecoverableTransactionManager;
import org.apache.geronimo.transaction.manager.NamedXAResource;
import org.apache.geronimo.transaction.manager.WrapperNamedXAResource;


/**
 * This class allows wiring the ActiveMQ broker and the Geronimo transaction manager
 * in a way that will allow the transaction manager to correctly recover XA transactions.
 *
 * For example, it can be used the following way:
 * <pre>
 *   <bean id="activemqConnectionFactory" class="org.apache.activemq.ActiveMQConnectionFactory">
 *      <property name="brokerURL" value="tcp://localhost:61616" />
 *   </bean>
 *
 *   <bean id="pooledConnectionFactory" class="org.apache.activemq.pool.PooledConnectionFactoryFactoryBean">
 *       <property name="maxConnections" value="8" />
 *       <property name="transactionManager" ref="transactionManager" />
 *       <property name="connectionFactory" ref="activemqConnectionFactory" />
 *       <property name="resourceName" value="activemq.broker" />
 *   </bean>
 *
 *   <bean id="resourceManager" class="org.apache.activemq.jms.pool.GenericResourceManager" init-method="recoverResource">
 *         <property name="transactionManager" ref="transactionManager" />
 *         <property name="connectionFactory" ref="activemqConnectionFactory" />
 *         <property name="resourceName" value="activemq.broker" />
 *   </bean>
 * </pre>
 */
public class GenericResourceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericResourceManager.class);

    private String resourceName;

    private String userName;
    private String password;

    private TransactionManager transactionManager;

    private ConnectionFactory connectionFactory;

    public void recoverResource() {
        try {
            if (!Recovery.recover(this)) {
                LOGGER.info("Resource manager is unrecoverable");
            }
        } catch (NoClassDefFoundError e) {
            LOGGER.info("Resource manager is unrecoverable due to missing classes: " + e);
        } catch (Throwable e) {
            LOGGER.warn("Error while recovering resource manager", e);
        }
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * This class will ensure the broker is properly recovered when wired with
     * the Geronimo transaction manager.
     */
    public static class Recovery {

        public static boolean isRecoverable(GenericResourceManager rm) {
            return  rm.getConnectionFactory() instanceof XAConnectionFactory &&
                    rm.getTransactionManager() instanceof RecoverableTransactionManager &&
                    rm.getResourceName() != null && !"".equals(rm.getResourceName());
        }

        public static boolean recover(final GenericResourceManager rm) throws IOException {
            if (isRecoverable(rm)) {
                final XAConnectionFactory connFactory = (XAConnectionFactory) rm.getConnectionFactory();

                RecoverableTransactionManager rtxManager = (RecoverableTransactionManager) rm.getTransactionManager();
                rtxManager.registerNamedXAResourceFactory(new NamedXAResourceFactory() {

                    @Override
                    public String getName() {
                        return rm.getResourceName();
                    }

                    @Override
                    public NamedXAResource getNamedXAResource() throws SystemException {
                        try {
                            final XAConnection xaConnection;
                            if (rm.getUserName() != null && rm.getPassword() != null) {
                                xaConnection = connFactory.createXAConnection(rm.getUserName(), rm.getPassword());
                            } else {
                                xaConnection = connFactory.createXAConnection();
                            }
                            final XASession session = xaConnection.createXASession();
                            xaConnection.start();
                            LOGGER.debug("new namedXAResource's connection: " + xaConnection);

                            return new ConnectionAndWrapperNamedXAResource(session.getXAResource(), getName(), xaConnection);
                        } catch (Exception e) {
                            SystemException se =  new SystemException("Failed to create ConnectionAndWrapperNamedXAResource, " + e.getLocalizedMessage());
                            se.initCause(e);
                            LOGGER.error(se.getLocalizedMessage(), se);
                            throw se;
                        }
                    }

                    @Override
                    public void returnNamedXAResource(NamedXAResource namedXaResource) {
                        if (namedXaResource instanceof ConnectionAndWrapperNamedXAResource) {
                            try {
                                LOGGER.debug("closing returned namedXAResource's connection: " + ((ConnectionAndWrapperNamedXAResource)namedXaResource).connection);
                                ((ConnectionAndWrapperNamedXAResource)namedXaResource).connection.close();
                            } catch (Exception ignored) {
                                LOGGER.debug("failed to close returned namedXAResource: " + namedXaResource, ignored);
                            }
                        }
                    }
                });
                return true;
            } else {
                return false;
            }
        }
    }

    public static class ConnectionAndWrapperNamedXAResource extends WrapperNamedXAResource {
        final Connection connection;
        public ConnectionAndWrapperNamedXAResource(XAResource xaResource, String name, Connection connection) {
            super(xaResource, name);
            this.connection = connection;
        }
    }
}
