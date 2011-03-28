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

import java.net.URI;

import javax.jms.JMSException;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * @author Oliver Belikan
 * 
 */
public class StartAndStopBrokerTest extends TestCase {
    public void testStartupShutdown() throws Exception {
        // This systemproperty is used if we dont want to
        // have persistence messages as a default
        System.setProperty("activemq.persistenceAdapter",
                "org.apache.activemq.store.vm.VMPersistenceAdapter");

        // configuration of container and all protocolls
        BrokerService broker = createBroker();

        // start a client
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:9100");
        factory.createConnection();

        // stop activemq broker
        broker.stop();

        // start activemq broker again
        broker = createBroker();

        // start a client again
        factory = new ActiveMQConnectionFactory("tcp://localhost:9100");
        factory.createConnection();

        // stop activemq broker
        broker.stop();

    }

    protected BrokerService createBroker() throws JMSException {
        BrokerService broker = null;

        try {
            broker = BrokerFactory.createBroker(new URI("broker://()/localhost"));
            broker.setBrokerName("DefaultBroker");
            broker.addConnector("tcp://localhost:9100");
            broker.setUseShutdownHook(false);
            
            broker.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return broker;
    }

}
