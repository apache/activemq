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
package org.apache.activemq.camel.component.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.broker.view.MessageBrokerView;
import org.apache.activemq.broker.view.MessageBrokerViewRegistry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.camel.ComponentConfiguration;
import org.apache.camel.Endpoint;
import org.apache.camel.component.jms.JmsConfiguration;
import org.apache.camel.impl.UriEndpointComponent;
import org.apache.camel.spi.EndpointCompleter;

import static org.apache.camel.util.ObjectHelper.removeStartingCharacters;

/**
 * The <a href="http://activemq.apache.org/broker-camel-component.html">Broker Camel component</a> allows to use Camel
 * routing to move messages through the broker.
 */
public class BrokerComponent extends UriEndpointComponent implements EndpointCompleter {

    public BrokerComponent() {
        super(BrokerEndpoint.class);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration();
        setProperties(brokerConfiguration, parameters);

        byte destinationType = ActiveMQDestination.QUEUE_TYPE;

        if (remaining.startsWith(JmsConfiguration.QUEUE_PREFIX)) {
            remaining = removeStartingCharacters(remaining.substring(JmsConfiguration.QUEUE_PREFIX.length()), '/');
        } else if (remaining.startsWith(JmsConfiguration.TOPIC_PREFIX)) {
            destinationType = ActiveMQDestination.TOPIC_TYPE;
            remaining = removeStartingCharacters(remaining.substring(JmsConfiguration.TOPIC_PREFIX.length()), '/');
        } else if (remaining.startsWith(JmsConfiguration.TEMP_QUEUE_PREFIX)) {
            destinationType = ActiveMQDestination.TEMP_QUEUE_TYPE;
            remaining = removeStartingCharacters(remaining.substring(JmsConfiguration.TEMP_QUEUE_PREFIX.length()), '/');
        } else if (remaining.startsWith(JmsConfiguration.TEMP_TOPIC_PREFIX)) {
            destinationType = ActiveMQDestination.TEMP_TOPIC_TYPE;
            remaining = removeStartingCharacters(remaining.substring(JmsConfiguration.TEMP_TOPIC_PREFIX.length()), '/');
        }

        ActiveMQDestination destination = ActiveMQDestination.createDestination(remaining, destinationType);
        BrokerEndpoint brokerEndpoint = new BrokerEndpoint(uri, this, remaining, destination, brokerConfiguration);
        setProperties(brokerEndpoint, parameters);
        return brokerEndpoint;
    }

    @Override
    public List<String> completeEndpointPath(ComponentConfiguration componentConfiguration, String completionText) {
        String brokerName = String.valueOf(componentConfiguration.getParameter("brokerName"));
        MessageBrokerView messageBrokerView = MessageBrokerViewRegistry.getInstance().lookup(brokerName);
        if (messageBrokerView != null) {
            String destinationName = completionText;
            Set<? extends ActiveMQDestination> set = messageBrokerView.getQueues();
            if (completionText.startsWith("topic:")) {
                set = messageBrokerView.getTopics();
                destinationName = completionText.substring(6);
            } else if (completionText.startsWith("queue:")) {
                destinationName = completionText.substring(6);
            }
            ArrayList<String> answer = new ArrayList<String>();
            for (ActiveMQDestination destination : set) {
                if (destination.getPhysicalName().startsWith(destinationName)) {
                    answer.add(destination.getPhysicalName());
                }
            }
            return answer;

        }
        return null;
    }
}
