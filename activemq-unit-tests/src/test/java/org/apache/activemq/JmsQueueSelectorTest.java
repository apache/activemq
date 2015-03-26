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

import javax.jms.*;

/**
 * 
 */
public class JmsQueueSelectorTest extends JmsTopicSelectorTest {
    public void setUp() throws Exception {
        topic = false;
        super.setUp();
    }

    public void testRedeliveryWithSelectors() throws Exception {
        consumer = createConsumer("");

        // send a message that would go to this consumer, but not to the next consumer we open
        TextMessage message = session.createTextMessage("1");
        message.setIntProperty("id", 1);
        message.setJMSType("b");
        message.setStringProperty("stringProperty", "b");
        message.setLongProperty("longProperty", 1);
        message.setBooleanProperty("booleanProperty", true);
        producer.send(message);

        // don't consume any messages.. close the consumer so that messages that had
        // been dispatched get marked as delivered, and queued for redelivery
        consumer.close();

        // send a message that will match the selector for the next consumer
        message = session.createTextMessage("1");
        message.setIntProperty("id", 1);
        message.setJMSType("a");
        message.setStringProperty("stringProperty", "a");
        message.setLongProperty("longProperty", 1);
        message.setBooleanProperty("booleanProperty", true);
        producer.send(message);

        consumer = createConsumer("stringProperty = 'a' and longProperty = 1 and booleanProperty = true");

        // now we, should only receive 1 message, not two
        int remaining = 2;

        javax.jms.Message recievedMsg = null;

        while (true) {
            recievedMsg = consumer.receive(1000);
            if (recievedMsg == null) {
                break;
            }
            String text = ((TextMessage)recievedMsg).getText();
            if (!text.equals("1") && !text.equals("3")) {
                fail("unexpected message: " + text);
            }
            remaining--;
        }

        assertEquals(1, remaining);
        consumer.close();
        consumeMessages(remaining);

    }
}
