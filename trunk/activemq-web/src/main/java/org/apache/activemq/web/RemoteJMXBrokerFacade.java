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
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.web.config.WebConsoleConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BrokerFacade} which uses a JMX-Connection to communicate with a
 * broker
 * 
 * 
 */
public class RemoteJMXBrokerFacade extends BrokerFacadeSupport {
    
    private static final transient Logger LOG = LoggerFactory.getLogger(RemoteJMXBrokerFacade.class);
    
    private String brokerName;
    private JMXConnector connector;
    private WebConsoleConfiguration configuration;

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public WebConsoleConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(WebConsoleConfiguration configuration) {
		this.configuration = configuration;
	}

	/**
     * Shutdown this facade aka close any open connection.
     */
    public void shutdown() {
        closeConnection();
    }
    
    private ObjectName getBrokerObjectName(MBeanServerConnection connection)
			throws IOException, MalformedObjectNameException {
		Set<ObjectName> brokers = findBrokers(connection);
		if (brokers.size() == 0) {
			throw new IOException("No broker could be found in the JMX.");
		}
		ObjectName name = brokers.iterator().next();
		return name;
	}

    public BrokerViewMBean getBrokerAdmin() throws Exception {
        MBeanServerConnection connection = getMBeanServerConnection();

        Set brokers = findBrokers(connection);
        if (brokers.size() == 0) {
            throw new IOException("No broker could be found in the JMX.");
        }
        ObjectName name = (ObjectName)brokers.iterator().next();
        BrokerViewMBean mbean = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(connection, name, BrokerViewMBean.class, true);
        return mbean;
    }

    public String getBrokerName() throws Exception,
			MalformedObjectNameException {
        return getBrokerAdmin().getBrokerName();
    }
    
    protected MBeanServerConnection getMBeanServerConnection() throws Exception {
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

        Map<String, Object> env = new HashMap<String, Object>();
		if (this.configuration.getJmxUser() != null) {
			env.put("jmx.remote.credentials", new String[] {
					this.configuration.getJmxUser(),
					this.configuration.getJmxPassword() });
		}
        Collection<JMXServiceURL> jmxUrls = this.configuration.getJmxUrls();

        Exception exception = null;
		for (JMXServiceURL url : jmxUrls) {
			try {
				JMXConnector connector = JMXConnectorFactory.connect(url, env);
				connector.connect();
				MBeanServerConnection connection = connector
						.getMBeanServerConnection();

				Set<ObjectName> brokers = findBrokers(connection);
				if (brokers.size() > 0) {
					LOG.info("Connected via JMX to the broker at " + url);
					return connector;
				}
			} catch (Exception e) {
				// Keep the exception for later
				exception = e;
			}
		}
		if (exception != null) {
			if (exception instanceof RuntimeException) {
				throw (RuntimeException) exception;
			} else {
				throw new RuntimeException(exception);
			}
		}
		throw new IllegalStateException("No broker is found at any of the "
				+ jmxUrls.size() + " configured urls");
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
	 * @param connection
	 *            not <code>null</code>
	 * @return Set with ObjectName-elements
	 * @throws IOException
	 * @throws MalformedObjectNameException
	 */
	@SuppressWarnings("unchecked")
	protected Set<ObjectName> findBrokers(MBeanServerConnection connection)
			throws IOException, MalformedObjectNameException {
		ObjectName name;
		if (this.brokerName == null) {
			name = new ObjectName("org.apache.activemq:Type=Broker,*");
		} else {
			name = new ObjectName("org.apache.activemq:BrokerName="
					+ this.brokerName + ",Type=Broker");
		}

		Set<ObjectName> brokers = connection.queryNames(name, null);
		return brokers;
	}
	
	public void purgeQueue(ActiveMQDestination destination) throws Exception {
		QueueViewMBean queue = getQueue(destination.getPhysicalName());
		queue.purge();
	}
	
	public ManagementContext getManagementContext() {
		throw new IllegalStateException("not supported");
	}

	
	@SuppressWarnings("unchecked")
	protected <T> Collection<T> getManagedObjects(ObjectName[] names,
			Class<T> type) {
		MBeanServerConnection connection;
		try {
			connection = getMBeanServerConnection();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		List<T> answer = new ArrayList<T>();
		if (connection != null) {
			for (int i = 0; i < names.length; i++) {
				ObjectName name = names[i];
				T value = (T) MBeanServerInvocationHandler.newProxyInstance(
						connection, name, type, true);
				if (value != null) {
					answer.add(value);
				}
			}
		}
		return answer;
    }

    @Override
    public Set queryNames(ObjectName name, QueryExp query) throws Exception {
        return getMBeanServerConnection().queryNames(name, query);
    }

    @Override
    public Object newProxyInstance(ObjectName objectName, Class interfaceClass,boolean notificationBroadcaster) throws Exception {
        return MBeanServerInvocationHandler.newProxyInstance(getMBeanServerConnection(), objectName, interfaceClass, notificationBroadcaster);
    }

}
