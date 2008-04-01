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
package org.apache.activemq.camel.component;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import javax.jms.ConnectionFactory;

import org.apache.activemq.spring.ActiveMQConnectionFactory;
import org.apache.camel.component.jms.JmsConfiguration;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

/**
 * @version $Revision$
 */
public class ActiveMQConfiguration extends JmsConfiguration {
    private String brokerURL = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;
    private boolean useSingleConnection = true;
    private boolean usePooledConnection = false;

    public ActiveMQConfiguration() {
    }

    public String getBrokerURL() {
        return brokerURL;
    }

    /**
     * Sets the broker URL to use to connect to ActiveMQ using the
     * <a href="http://activemq.apache.org/configuring-transports.html">ActiveMQ URI format</a>
     *
     * @param brokerURL the URL of the broker.
     */
    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }

    public boolean isUseSingleConnection() {
        return useSingleConnection;
    }

    /**
     * Enables or disables whether a Spring {@link SingleConnectionFactory} will be used so that when
     * messages are sent to ActiveMQ from outside of a message consuming thread, pooling will be used rather
     * than the default with the Spring {@link JmsTemplate} which will create a new connection, session, producer
     * for each message then close them all down again.
     * <p/>
     * The default value is true so that a single connection is used by default.
     *
     * @param useSingleConnection
     */
    public void setUseSingleConnection(boolean useSingleConnection) {
        this.useSingleConnection = useSingleConnection;
    }

    public boolean isUsePooledConnection() {
        return usePooledConnection;
    }

    /**
     * Enables or disables whether a PooledConnectionFactory will be used so that when
     * messages are sent to ActiveMQ from outside of a message consuming thread, pooling will be used rather
     * than the default with the Spring {@link JmsTemplate} which will create a new connection, session, producer
     * for each message then close them all down again.
     * <p/>
     * The default value is false by default as it requires an extra dependency on commons-pool.
     */
    public void setUsePooledConnection(boolean usePooledConnection) {
        this.usePooledConnection = usePooledConnection;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() {
        ActiveMQConnectionFactory answer = new ActiveMQConnectionFactory();
        if (answer.getBeanName() == null) {
            answer.setBeanName("Camel");
        }
        answer.setBrokerURL(getBrokerURL());
        if (isUsePooledConnection()) {
            return createPooledConnectionFactory(answer);
        }
        else if (isUseSingleConnection()) {
            return new SingleConnectionFactory(answer);
        }
        else {
            return answer;
        }
    }

    protected ConnectionFactory createPooledConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        // lets not use classes directly to avoid a runtime dependency on commons-pool
        // for folks not using this option
        try {
            Class type = loadClass("org.apache.activemq.pool.PooledConnectionFactory", getClass().getClassLoader());
            Constructor constructor = type.getConstructor(org.apache.activemq.ActiveMQConnectionFactory.class);
            return (ConnectionFactory) constructor.newInstance(connectionFactory);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to instantiate PooledConnectionFactory: " + e, e);
        }
    }

    public static Class<?> loadClass(String name, ClassLoader loader) throws ClassNotFoundException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            try {
                return contextClassLoader.loadClass(name);
            }
            catch (ClassNotFoundException e) {
                try {
                    return loader.loadClass(name);
                }
                catch (ClassNotFoundException e1) {
                    throw e1;
                }
            }
        }
        return null;
    }
}
