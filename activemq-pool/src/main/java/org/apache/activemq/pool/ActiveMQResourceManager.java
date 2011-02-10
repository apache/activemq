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

import java.io.IOException;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.transaction.TransactionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.util.IOExceptionSupport;
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
 *   <bean id="resourceManager" class="org.apache.activemq.pool.ActiveMQResourceManager" init-method="recoverResource">
 *         <property name="transactionManager" ref="transactionManager" />
 *         <property name="connectionFactory" ref="activemqConnectionFactory" />
 *         <property name="resourceName" value="activemq.broker" />
 *   </bean>
 * </pre>
 */
public class ActiveMQResourceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQResourceManager.class);

    private String resourceName;

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

        public static boolean isRecoverable(ActiveMQResourceManager rm) {
            return  rm.getConnectionFactory() instanceof ActiveMQConnectionFactory &&
                    rm.getTransactionManager() instanceof RecoverableTransactionManager &&
                    rm.getResourceName() != null && !"".equals(rm.getResourceName());
        }

        public static boolean recover(ActiveMQResourceManager rm) throws IOException {
            if (isRecoverable(rm)) {
                try {
                    ActiveMQConnectionFactory connFactory = (ActiveMQConnectionFactory) rm.getConnectionFactory();
                    ActiveMQConnection activeConn = (ActiveMQConnection)connFactory.createConnection();
                    ActiveMQSession session = (ActiveMQSession)activeConn.createSession(true, Session.SESSION_TRANSACTED);
                    NamedXAResource namedXaResource = new WrapperNamedXAResource(session.getTransactionContext(), rm.getResourceName());

                    RecoverableTransactionManager rtxManager = (RecoverableTransactionManager) rm.getTransactionManager();
                    rtxManager.recoverResourceManager(namedXaResource);
                    return true;
                } catch (JMSException e) {
                  throw IOExceptionSupport.create(e);
                }
            } else {
                return false;
            }
        }
    }

}
