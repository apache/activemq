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

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.util.MessageIdList;
import org.springframework.core.io.ClassPathResource;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

public class TopicReplicationTest extends JmsMultipleBrokersTestSupport {

    public static final int MSG_COUNT = 10;

    public void testReplication() throws Exception {
        createBroker(new ClassPathResource("org/apache/activemq/usecases/replication-broker1.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/replication-broker2.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/replication-broker3.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/replication-broker4.xml"));

        brokers.get("replication-broker1").broker.waitUntilStarted();
        brokers.get("replication-broker2").broker.waitUntilStarted();
        brokers.get("replication-broker3").broker.waitUntilStarted();
        brokers.get("replication-broker4").broker.waitUntilStarted();

        Destination dest = createDestination("replication", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("replication-broker2", dest);
        MessageConsumer clientB = createConsumer("replication-broker3", dest);
        MessageConsumer clientC = createConsumer("replication-broker4", dest);
        MessageConsumer clientD = createConsumer("replication-broker4", dest);

        //let consumers propogate around the network
        Thread.sleep(2000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("replication-broker2", clientA);
        MessageIdList msgsB = getConsumerMessages("replication-broker3", clientB);
        MessageIdList msgsC = getConsumerMessages("replication-broker4", clientC);
        MessageIdList msgsD = getConsumerMessages("replication-broker4", clientD);



        // send messages to broker1
        sendMessages("replication-broker1", dest, MSG_COUNT);


        msgsA.waitForMessagesToArrive(MSG_COUNT);
        msgsB.waitForMessagesToArrive(MSG_COUNT);
        msgsC.waitForMessagesToArrive(MSG_COUNT);
        msgsD.waitForMessagesToArrive(MSG_COUNT);

        assertEquals(MSG_COUNT, msgsA.getMessageCount());
        assertEquals(MSG_COUNT, msgsB.getMessageCount());
        assertEquals(MSG_COUNT, msgsC.getMessageCount());
        assertEquals(MSG_COUNT, msgsD.getMessageCount());

        // send messages to broker4
        sendMessages("replication-broker4", dest, MSG_COUNT);

        msgsA.waitForMessagesToArrive(2 * MSG_COUNT);
        msgsB.waitForMessagesToArrive(2 * MSG_COUNT);
        msgsC.waitForMessagesToArrive(2 * MSG_COUNT);
        msgsD.waitForMessagesToArrive(2 * MSG_COUNT);

        assertEquals(2 * MSG_COUNT, msgsA.getMessageCount());
        assertEquals(2 * MSG_COUNT, msgsB.getMessageCount());
        assertEquals(2 * MSG_COUNT, msgsC.getMessageCount());
        assertEquals(2 * MSG_COUNT, msgsD.getMessageCount());

        // send messages to broker3
        sendMessages("replication-broker3", dest, MSG_COUNT);

        msgsA.waitForMessagesToArrive(3 * MSG_COUNT);
        msgsB.waitForMessagesToArrive(3 * MSG_COUNT);
        msgsC.waitForMessagesToArrive(3 * MSG_COUNT);
        msgsD.waitForMessagesToArrive(3 * MSG_COUNT);

        assertEquals(3 * MSG_COUNT, msgsA.getMessageCount());
        assertEquals(3 * MSG_COUNT, msgsB.getMessageCount());
        assertEquals(3 * MSG_COUNT, msgsC.getMessageCount());
        assertEquals(3 * MSG_COUNT, msgsD.getMessageCount());

        // send messages to broker2
        sendMessages("replication-broker2", dest, MSG_COUNT);

        msgsA.waitForMessagesToArrive(4 * MSG_COUNT);
        msgsB.waitForMessagesToArrive(4 * MSG_COUNT);
        msgsC.waitForMessagesToArrive(4 * MSG_COUNT);
        msgsD.waitForMessagesToArrive(4 * MSG_COUNT);

        assertEquals(4 * MSG_COUNT, msgsA.getMessageCount());
        assertEquals(4 * MSG_COUNT, msgsB.getMessageCount());
        assertEquals(4 * MSG_COUNT, msgsC.getMessageCount());
        assertEquals(4 * MSG_COUNT, msgsD.getMessageCount());

    }


}
