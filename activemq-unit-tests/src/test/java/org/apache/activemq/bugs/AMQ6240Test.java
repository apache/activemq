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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.RequestTimedOutIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AMQ6240Test extends JmsTimeoutTest {

    static final Logger LOG = LoggerFactory.getLogger(AMQ6240Test.class);


    public boolean isPersistent() { return true;}

    public void testBlockedTxProducerConnectionTimeoutConnectionCanClose() throws Exception {
        final ActiveMQConnection cx = (ActiveMQConnection)createConnection();
        final ActiveMQDestination queue = createDestination("noPfc");

        // we should not take longer than 10 seconds to return from send
        cx.setSendTimeout(10000);

        Runnable r = new Runnable() {
            public void run() {
                try {
                    LOG.info("Sender thread starting");
                    Session session = cx.createSession(true, Session.SESSION_TRANSACTED);
                    MessageProducer producer = session.createProducer(queue);
                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);

                    TextMessage message = session.createTextMessage(createMessageText());
                    for(int count=0; count<messageCount; count++){
                        producer.send(message);
                    }
                    LOG.info("Done sending..");
                } catch (JMSException e) {
                    if (e.getCause() instanceof RequestTimedOutIOException) {
                        exceptionCount.incrementAndGet();
                    } else {
                        e.printStackTrace();
                    }
                    return;
                }

            }
        };
        cx.start();
        Thread producerThread = new Thread(r);
        producerThread.start();
        producerThread.join(15000);
        cx.close();
        // We should have a few timeout exceptions as memory store will fill up
        assertTrue("No exception from the broker", exceptionCount.get() > 0);
    }


    protected void setUp() throws Exception {
        super.setUp();

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry noProducerFlowControl = new PolicyEntry();
        noProducerFlowControl.setProducerFlowControl(false);
        policyMap.put(new ActiveMQQueue("noPfc"), noProducerFlowControl);
        broker.setDestinationPolicy(policyMap);
        broker.getSystemUsage().getStoreUsage().setLimit(50*1024*1024);

    }
}
