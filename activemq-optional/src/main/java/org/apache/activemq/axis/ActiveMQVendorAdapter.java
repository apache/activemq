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
package org.apache.activemq.axis;

import java.util.HashMap;

import javax.jms.ConnectionFactory;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.axis.components.jms.BeanVendorAdapter;
import org.apache.axis.transport.jms.JMSURLHelper;

/**
 * An adapter for using ActiveMQ inside <a
 * href="http://ws.apache.org/axis/">Apache Axis</a>
 * 
 * @version $Revision$
 */
public class ActiveMQVendorAdapter extends BeanVendorAdapter {

    /**
     * The URL to connect to the broker
     */
    public static final String BROKER_URL = "brokerURL";

    /**
     * Specifies the default user name
     */
    public static final String DEFAULT_USERNAME = "defaultUser";

    /**
     * Specifies the default password
     */
    public static final String DEFAULT_PASSWORD = "defaultPassword";

    protected static final String QCF_CLASS = ActiveMQConnectionFactory.class.getName();
    protected static final String TCF_CLASS = QCF_CLASS;


    public QueueConnectionFactory getQueueConnectionFactory(HashMap properties) throws Exception {
        properties = (HashMap)properties.clone();
        properties.put(CONNECTION_FACTORY_CLASS, QCF_CLASS);
        return super.getQueueConnectionFactory(properties);
    }

    public TopicConnectionFactory getTopicConnectionFactory(HashMap properties) throws Exception {
        properties = (HashMap)properties.clone();
        properties.put(CONNECTION_FACTORY_CLASS, TCF_CLASS);
        return super.getTopicConnectionFactory(properties);
    }

    public void addVendorConnectionFactoryProperties(JMSURLHelper jmsUrl, HashMap properties) {
        if (jmsUrl.getPropertyValue(BROKER_URL) != null) {
            properties.put(BROKER_URL, jmsUrl.getPropertyValue(BROKER_URL));
        }

        if (jmsUrl.getPropertyValue(DEFAULT_USERNAME) != null) {
            properties.put(DEFAULT_USERNAME, jmsUrl.getPropertyValue(DEFAULT_USERNAME));
        }
        if (jmsUrl.getPropertyValue(DEFAULT_PASSWORD) != null) {
            properties.put(DEFAULT_PASSWORD, jmsUrl.getPropertyValue(DEFAULT_PASSWORD));
        }
    }

    public boolean isMatchingConnectionFactory(ConnectionFactory connectionFactory, JMSURLHelper jmsURL, HashMap properties) {
        String brokerURL = null;

        if (connectionFactory instanceof ActiveMQConnectionFactory) {
            ActiveMQConnectionFactory amqConnectionFactory = (ActiveMQConnectionFactory)connectionFactory;

            // get existing queue connection factory properties
            brokerURL = amqConnectionFactory.getBrokerURL();
        }

        // compare broker url
        String propertyBrokerURL = (String)properties.get(BROKER_URL);
        if (brokerURL == null || !brokerURL.equals(propertyBrokerURL)) {
            return false;
        }
        return true;
    }
}
