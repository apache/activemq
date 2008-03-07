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

import org.apache.camel.CamelContext;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.jms.JmsConfiguration;

/**
 * The <a href="http://activemq.apache.org/camel/activemq.html">ActiveMQ Component</a>
 *
 * @version $Revision$
 */
public class ActiveMQComponent extends JmsComponent {
    private boolean exposeAllQueues;
    private CamelEndpointLoader endpointLoader;

    /**
     * Creates an <a href="http://activemq.apache.org/camel/activemq.html">ActiveMQ Component</a>
     *
     * @return the created component
     */
    public static ActiveMQComponent activeMQComponent() {
        return new ActiveMQComponent();
    }

    /**
     * Creates an <a href="http://activemq.apache.org/camel/activemq.html">ActiveMQ Component</a>
     * connecting to the given <a href="http://activemq.apache.org/configuring-transports.html">broker URL</a>
     *
     * @param brokerURL the URL to connect to
     * @return the created component
     */
    public static ActiveMQComponent activeMQComponent(String brokerURL) {
        ActiveMQComponent answer = new ActiveMQComponent();
        answer.getConfiguration().setBrokerURL(brokerURL);
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

    @Override
    public ActiveMQConfiguration getConfiguration() {
        return (ActiveMQConfiguration) super.getConfiguration();
    }

    public void setBrokerURL(String brokerURL) {
        getConfiguration().setBrokerURL(brokerURL);
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
        super.doStop();
    }

    @Override
    protected JmsConfiguration createConfiguration() {
        return new ActiveMQConfiguration();
    }
}
