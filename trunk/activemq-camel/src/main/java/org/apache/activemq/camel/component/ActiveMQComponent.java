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

import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.camel.CamelContext;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.jms.JmsConfiguration;
import org.springframework.jms.connection.SingleConnectionFactory;

/**
 * The <a href="http://activemq.apache.org/camel/activemq.html">ActiveMQ Component</a>
 *
 * 
 */
public class ActiveMQComponent extends JmsComponent {
    private final CopyOnWriteArrayList<SingleConnectionFactory> singleConnectionFactoryList =
        new CopyOnWriteArrayList<SingleConnectionFactory>();
    private final CopyOnWriteArrayList<Service> pooledConnectionFactoryServiceList =
        new CopyOnWriteArrayList<Service>();
    private boolean exposeAllQueues;
    private CamelEndpointLoader endpointLoader;

    /**
     * Creates an <a href="http://camel.apache.org/activemq.html">ActiveMQ Component</a>
     *
     * @return the created component
     */
    public static ActiveMQComponent activeMQComponent() {
        return new ActiveMQComponent();
    }

    /**
     * Creates an <a href="http://camel.apache.org/activemq.html">ActiveMQ Component</a>
     * connecting to the given <a href="http://activemq.apache.org/configuring-transports.html">broker URL</a>
     *
     * @param brokerURL the URL to connect to
     * @return the created component
     */
    public static ActiveMQComponent activeMQComponent(String brokerURL) {
        ActiveMQComponent answer = new ActiveMQComponent();
        if (answer.getConfiguration() instanceof ActiveMQConfiguration) {
            ((ActiveMQConfiguration) answer.getConfiguration())
                    .setBrokerURL(brokerURL);
        }

        // set the connection factory with the provided broker url
        answer.setConnectionFactory(new ActiveMQConnectionFactory(brokerURL));
        return answer;
    }

    public ActiveMQComponent() {
    }

    public ActiveMQComponent(CamelContext context) {
        super(context);
    }

    public ActiveMQComponent(ActiveMQConfiguration configuration) {
        super(configuration);
    }


    public void setBrokerURL(String brokerURL) {
        if (getConfiguration() instanceof ActiveMQConfiguration) {
            ((ActiveMQConfiguration)getConfiguration()).setBrokerURL(brokerURL);
        }
    }

    public void setUserName(String userName) {
        if (getConfiguration() instanceof ActiveMQConfiguration) {
            ((ActiveMQConfiguration)getConfiguration()).setUserName(userName);
        }
    }

    public void setPassword(String password) {
        if (getConfiguration() instanceof ActiveMQConfiguration) {
            ((ActiveMQConfiguration)getConfiguration()).setPassword(password);
        }
    }

    public boolean isExposeAllQueues() {
        return exposeAllQueues;
    }

    /**
     * If enabled this will cause all Queues in the ActiveMQ broker to be eagerly populated into the CamelContext
     * so that they can be easily browsed by any Camel tooling. This option is disabled by default.
     *
     * @param exposeAllQueues
     */
    public void setExposeAllQueues(boolean exposeAllQueues) {
        this.exposeAllQueues = exposeAllQueues;
    }

    public void setUsePooledConnection(boolean usePooledConnection) {
        if (getConfiguration() instanceof ActiveMQConfiguration) {
            ((ActiveMQConfiguration)getConfiguration()).setUsePooledConnection(usePooledConnection);
        }
    }

    public void setUseSingleConnection(boolean useSingleConnection) {
        if (getConfiguration() instanceof ActiveMQConfiguration) {
            ((ActiveMQConfiguration)getConfiguration()).setUseSingleConnection(useSingleConnection);
        }
    }

    protected void addPooledConnectionFactoryService(Service pooledConnectionFactoryService) {
        pooledConnectionFactoryServiceList.add(pooledConnectionFactoryService);
    }

    protected void addSingleConnectionFactory(SingleConnectionFactory singleConnectionFactory) {
        singleConnectionFactoryList.add(singleConnectionFactory);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (isExposeAllQueues()) {
            endpointLoader = new CamelEndpointLoader(getCamelContext());
            endpointLoader.afterPropertiesSet();
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (endpointLoader != null) {
            endpointLoader.destroy();
            endpointLoader = null;
        }
        for (Service s : pooledConnectionFactoryServiceList) {
            s.stop();
        }
        pooledConnectionFactoryServiceList.clear();
        for (SingleConnectionFactory s : singleConnectionFactoryList) {
            s.destroy();
        }
        singleConnectionFactoryList.clear();
        super.doStop();
    }

    @Override
    public void setConfiguration(JmsConfiguration configuration) {
        if (configuration instanceof ActiveMQConfiguration) {
            ((ActiveMQConfiguration) configuration).setActiveMQComponent(this);
        }
        super.setConfiguration(configuration);
    }

    @Override
    protected JmsConfiguration createConfiguration() {
        ActiveMQConfiguration answer = new ActiveMQConfiguration();
        answer.setActiveMQComponent(this);
        return answer;
    }
}
