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
package org.apache.activemq.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link BrokerFacade} which uses a JMX-Connection to communicate with a
 * broker
 * 
 * @version $Revision: 1.1 $
 */
public class RemoteJMXBrokerFacade extends BrokerFacadeSupport {
    
    private static final transient Log LOG = LogFactory.getLog(RemoteJMXBrokerFacade.class);
    
    private String jmxUrl;
    private String jmxRole;
    private String jmxPassword;
    private String brokerName;
    private JMXConnector connector;

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public void setJmxUrl(String url) {
        this.jmxUrl = url;
    }

    public void setJmxRole(String role) {
        this.jmxRole = role;
    }

    public void setJmxPassword(String password) {
        this.jmxPassword = password;
    }

    /**
     * Shutdown this facade aka close any open connection.
     */
    public void shutdown() {
        closeConnection();
    }

    public BrokerViewMBean getBrokerAdmin() throws Exception {
        MBeanServerConnection connection = getConnection();

        Set brokers = findBrokers(connection);
        if (brokers.size() == 0) {
            throw new IOException("No broker could be found in the JMX.");
        }
        ObjectName name = (ObjectName)brokers.iterator().next();
        BrokerViewMBean mbean = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(connection, name, BrokerViewMBean.class, true);
        return mbean;
    }

    protected MBeanServerConnection getConnection() throws IOException {
        JMXConnector connector = this.connector;
        if (isConnectionActive(connector)) {
            return connector.getMBeanServerConnection();
        }

        synchronized (this) {
            closeConnection();

            LOG.debug("Creating a new JMX-Connection to the broker");
            this.connector = createConnection();
            return this.connector.getMBeanServerConnection();
        }
    }

    protected boolean isConnectionActive(JMXConnector connector) {
        if (connector == null) {
            return false;
        }

        try {
            MBeanServerConnection connection = connector.getMBeanServerConnection();
            int brokerCount = findBrokers(connection).size();
            return brokerCount > 0;
        } catch (Exception e) {
            return false;
        }
    }

    protected JMXConnector createConnection() {
        String[] urls = this.jmxUrl.split(",");
        HashMap env = new HashMap();
        env.put("jmx.remote.credentials", new String[] {
            this.jmxRole, this.jmxPassword
        });

        if (urls == null || urls.length == 0) {
            urls = new String[] {
                this.jmxUrl
            };
        }

        Exception exception = null;
        for (int i = 0; i < urls.length; i++) {
            try {
                JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(urls[i]), env);
                connector.connect();
                MBeanServerConnection connection = connector.getMBeanServerConnection();

                Set brokers = findBrokers(connection);
                if (brokers.size() > 0) {
                    LOG.info("Connected via JMX to the broker at " + urls[i]);
                    return connector;
                }
            } catch (Exception e) {
                // Keep the exception for later
                exception = e;
            }
        }
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException)exception;
            } else {
                throw new RuntimeException(exception);
            }
        }
        throw new IllegalStateException("No broker is found at any of the urls " + this.jmxUrl);
    }

    protected synchronized void closeConnection() {
        if (connector != null) {
            try {
                LOG.debug("Closing a connection to a broker (" + connector.getConnectionId() + ")");

                connector.close();
            } catch (IOException e) {
                // Ignore the exception, since it most likly won't matter
                // anymore
            }
        }
    }

    /**
     * Finds all ActiveMQ-Brokers registered on a certain JMX-Server or, if a
     * JMX-BrokerName has been set, the broker with that name.
     * 
     * @param connection not <code>null</code>
     * @return Set with ObjectName-elements
     * @throws IOException
     * @throws MalformedObjectNameException
     */
    protected Set findBrokers(MBeanServerConnection connection) throws IOException, MalformedObjectNameException {
        ObjectName name;
        if (this.brokerName == null) {
            name = new ObjectName("org.apache.activemq:Type=Broker,*");
        } else {
            name = new ObjectName("org.apache.activemq:BrokerName=" + this.brokerName + ",Type=Broker");
        }

        Set brokers = connection.queryNames(name, null);
        return brokers;
    }

    public void purgeQueue(ActiveMQDestination destination) throws Exception {
        QueueViewMBean queue = getQueue(destination.getPhysicalName());
        queue.purge();
    }

    public ManagementContext getManagementContext() {
        throw new IllegalStateException("not supported");
    }

    protected Collection getManagedObjects(ObjectName[] names, Class type) {
        MBeanServerConnection connection;
        try {
            connection = getConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List answer = new ArrayList();
        if (connection != null) {
            for (int i = 0; i < names.length; i++) {
                ObjectName name = names[i];
                Object value = MBeanServerInvocationHandler.newProxyInstance(connection, name, type, true);
                if (value != null) {
                    answer.add(value);
                }
            }
        }
        return answer;
    }

}
