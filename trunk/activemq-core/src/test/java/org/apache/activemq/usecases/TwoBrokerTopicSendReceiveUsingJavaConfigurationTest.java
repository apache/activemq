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
package org.apache.activemq.usecases;

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * 
 */
public class TwoBrokerTopicSendReceiveUsingJavaConfigurationTest extends TwoBrokerTopicSendReceiveTest {
    BrokerService receiveBroker;
    BrokerService sendBroker;

    protected ActiveMQConnectionFactory createReceiverConnectionFactory() throws JMSException {
        try {
            receiveBroker = new BrokerService();
            receiveBroker.setBrokerName("receiveBroker");
            receiveBroker.setUseJmx(false);
            receiveBroker.setPersistent(false);
            receiveBroker.addConnector("tcp://localhost:62002");
            receiveBroker.addNetworkConnector("static:failover:tcp://localhost:62001");
            receiveBroker.start();

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:62002");
            return factory;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    protected ActiveMQConnectionFactory createSenderConnectionFactory() throws JMSException {
        try {
            sendBroker = new BrokerService();
            sendBroker.setBrokerName("sendBroker");
            sendBroker.setUseJmx(false);
            sendBroker.setPersistent(false);
            sendBroker.addConnector("tcp://localhost:62001");
            sendBroker.addNetworkConnector("static:failover:tcp://localhost:62002");
            sendBroker.start();

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:62001");
            return factory;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (sendBroker != null) {
            sendBroker.stop();
        }
        if (receiveBroker != null) {
            receiveBroker.stop();
        }
    }

}
