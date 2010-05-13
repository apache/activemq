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

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TwoBrokerNetworkLoadBalanceTest extends JmsMultipleBrokersTestSupport {
    protected static final Log LOG = LogFactory.getLog(TwoBrokerNetworkLoadBalanceTest.class);
    public void testLoadBalancing() throws Exception {
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerA");

        startAllBrokers();
        waitForBridgeFormation();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);

     // Setup consumers
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        
        // Send messages
        sendMessages("BrokerA", dest, 5000);

        // Send messages
        sendMessages("BrokerB", dest, 1000);

        // Get message count
        final MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        final MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return msgsA.getMessageCount() + msgsB.getMessageCount() == 6000;
            }});
        
        LOG.info("A got: " +  msgsA.getMessageCount());
        LOG.info("B got: " +  msgsB.getMessageCount());
         
        assertTrue("B got is fair share: " + msgsB.getMessageCount(), msgsB.getMessageCount() > 2000);
    }
    
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI(
                "broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI(
                "broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
    }
}
