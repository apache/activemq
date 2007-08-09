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
package org.apache.activemq;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Topic;

import org.apache.activemq.test.JmsTopicSendReceiveTest;


/**
 * @version $Revision: 1.3 $
 */
public class JmsTopicCompositeSendReceiveTest extends JmsTopicSendReceiveTest {
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(JmsTopicCompositeSendReceiveTest.class);
    
    Destination consumerDestination2;
    MessageConsumer consumer2;

    /**
     * Sets a test to have a queue destination and non-persistent delivery mode.
     *
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        super.setUp();
        consumerDestination2 = consumeSession.createTopic("FOO.BAR.HUMBUG2");
        log.info("Created  consumer destination: " + consumerDestination2 + " of type: " + consumerDestination2.getClass());
        if (durable) {
            log.info("Creating durable consumer");
            consumer2 = consumeSession.createDurableSubscriber((Topic) consumerDestination2, getName());
        } else {
            consumer2 = consumeSession.createConsumer(consumerDestination2);
        }

    }

    /**
     * Returns the consumer subject.
     *
     * @return String - consumer subject
     * @see org.apache.activemq.test.TestSupport#getConsumerSubject()
     */
    protected String getConsumerSubject() {
        return "FOO.BAR.HUMBUG";
    }

    /**
     * Returns the producer subject.
     *
     * @return String - producer subject
     * @see org.apache.activemq.test.TestSupport#getProducerSubject()
     */
    protected String getProducerSubject() {
        return "FOO.BAR.HUMBUG,FOO.BAR.HUMBUG2";
    }

    /**
     * Test if all the messages sent are being received.
     *
     * @throws Exception
     */
    public void testSendReceive() throws Exception {
        super.testSendReceive();
        messages.clear();
        consumer2.setMessageListener(this);
        assertMessagesAreReceived();
        log.info("" + data.length + " messages(s) received, closing down connections");
    }
}
