/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.activemq.usecases;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.activemq.broker.TransportConnector;
import org.apache.activemq.activemq.broker.BrokerService;
import org.apache.activemq.activemq.xbean.BrokerFactoryBean;
import org.apache.activemq.activemq.test.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.springframework.core.io.ClassPathResource;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class TwoBrokerTopicSendReceiveTest extends JmsTopicSendReceiveWithTwoConnectionsTest {

    protected ActiveMQConnectionFactory sendFactory;
    protected ActiveMQConnectionFactory receiveFactory;

    protected void setUp() throws Exception {
        sendFactory = createSenderConnectionFactory();
        receiveFactory = createReceiverConnectionFactory();
        super.setUp();
    }

    protected ActiveMQConnectionFactory createReceiverConnectionFactory() throws JMSException {
        return createConnectionFactory("org/activemq/usecases/receiver.xml", "receiver", "vm://receiver");
    }

    protected ActiveMQConnectionFactory createSenderConnectionFactory() throws JMSException {
        return createConnectionFactory("org/activemq/usecases/sender.xml", "sender", "vm://sender");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected Connection createReceiveConnection() throws JMSException {
        return receiveFactory.createConnection();
    }

    protected Connection createSendConnection() throws JMSException {
        return sendFactory.createConnection();
    }

    protected ActiveMQConnectionFactory createConnectionFactory(String config, String brokerName, String connectUrl) throws JMSException {
        try {
            BrokerFactoryBean brokerFactory = new BrokerFactoryBean(new ClassPathResource(config));
            brokerFactory.afterPropertiesSet();

            BrokerService broker = brokerFactory.getBroker();
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(((TransportConnector)broker.getTransportConnectors().get(0)).getConnectUri());

            return factory;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
