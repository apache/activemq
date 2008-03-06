/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.camel.component;

import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.advisory.DestinationEvent;
import org.apache.activemq.advisory.DestinationListener;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Endpoint;
import org.apache.camel.component.jms.JmsEndpoint;
import org.apache.camel.util.ObjectHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

/**
 * A helper bean which populates a {@link CamelContext} with ActiveMQ Queue endpoints
 * 
 * @version $Revision: 1.1 $
 */
public class CamelEndpointLoader implements InitializingBean, CamelContextAware {
    private static final transient Log LOG = LogFactory.getLog(CamelEndpointLoader.class);
    private CamelContext camelContext;
    private ActiveMQConnection connection;
    private ConnectionFactory connectionFactory;
    private ActiveMQComponent component;

    public CamelEndpointLoader() {
    }

    public CamelEndpointLoader(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public CamelContext getCamelContext() {
        return camelContext;
    }

    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public ActiveMQConnection getConnection() {
        return connection;
    }

    public ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            connectionFactory = getComponent().getConfiguration().createConnectionFactory();
        }
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public ActiveMQComponent getComponent() {
        if (component == null) {
            component = camelContext.getComponent("activemq", ActiveMQComponent.class);
        }
        return component;
    }

    public void setComponent(ActiveMQComponent component) {
        this.component = component;
    }

    public void afterPropertiesSet() throws Exception {
        ObjectHelper.notNull(camelContext, "camelContext");
        if (connection == null) {
            Connection value = getConnectionFactory().createConnection();
            if (value instanceof ActiveMQConnection) {
                connection = (ActiveMQConnection) value;
            }
            else {
                throw new IllegalArgumentException("Created JMS Connection is not an ActiveMQConnection: " + value);
            }
        }
        DestinationSource source = connection.getDestinationSource();
        source.setDestinationListener(new DestinationListener() {
            public void onDestinationEvent(DestinationEvent event) {
                try {
                    ActiveMQDestination destination = event.getDestination();
                    if (destination instanceof ActiveMQQueue) {
                        ActiveMQQueue queue = (ActiveMQQueue) destination;
                        if (event.isAddOperation()) {
                            addQueue(queue);
                        }
                        else {
                            removeQueue(queue);
                        }
                    }
                }
                catch (Exception e) {
                    LOG.warn("Caught: " + e, e);
                }
            }
        });

        Set<ActiveMQQueue> queues = source.getQueues();
        for (ActiveMQQueue queue : queues) {
            addQueue(queue);
        }
    }

    protected void addQueue(ActiveMQQueue queue) throws Exception {
        String queueUri = getQueueUri(queue);
        ActiveMQComponent jmsComponent = getComponent();
        Endpoint endpoint = new JmsEndpoint(queueUri, jmsComponent, queue.getPhysicalName(), false, jmsComponent.getConfiguration());
        camelContext.addSingletonEndpoint(queueUri, endpoint);
    }

    protected String getQueueUri(ActiveMQQueue queue) {
        return "activemq:" + queue.getPhysicalName();
    }

    protected void removeQueue(ActiveMQQueue queue) throws Exception {
        String queueUri = getQueueUri(queue);
        camelContext.removeSingletonEndpoint(queueUri);
    }
}
