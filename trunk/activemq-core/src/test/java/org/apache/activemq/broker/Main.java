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
package org.apache.activemq.broker;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.demo.DefaultQueueSender;

/**
 * A helper class which can be handy for running a broker in your IDE from the
 * activemq-core module.
 * 
 * 
 */
public final class Main {
    protected static boolean createConsumers;

    private Main() {        
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            // TODO - this seems to break interceptors for some reason
            // BrokerService broker = BrokerFactory.createBroker(new
            // URI(brokerURI));
            BrokerService broker = new BrokerService();
            broker.setPersistent(false);

            // for running on Java 5 without mx4j
            ManagementContext managementContext = broker.getManagementContext();
            managementContext.setFindTigerMbeanServer(true);
            managementContext.setUseMBeanServer(true);
            managementContext.setCreateConnector(false);

            broker.setUseJmx(true);
            // broker.setPlugins(new BrokerPlugin[] { new
            // ConnectionDotFilePlugin(), new UDPTraceBrokerPlugin() });
            broker.addConnector("tcp://localhost:61616");
            broker.addConnector("stomp://localhost:61613");
            broker.start();

            // lets publish some messages so that there is some stuff to browse
            DefaultQueueSender.main(new String[] {"Prices.Equity.IBM"});
            DefaultQueueSender.main(new String[] {"Prices.Equity.MSFT"});

            // lets create a dummy couple of consumers
            if (createConsumers) {
                Connection connection = new ActiveMQConnectionFactory().createConnection();
                connection.start();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                session.createConsumer(new ActiveMQQueue("Orders.IBM"));
                session.createConsumer(new ActiveMQQueue("Orders.MSFT"), "price > 100");
                Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                session2.createConsumer(new ActiveMQQueue("Orders.MSFT"), "price > 200");
            } else {
                // Lets wait for the broker
                broker.waitUntilStopped();
            }
        } catch (Exception e) {
            System.out.println("Failed: " + e);
            e.printStackTrace();
        }
    }
}
