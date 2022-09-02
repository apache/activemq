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
package org.apache.activemq.jms2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ActiveMQJMS2AckModesTest extends ActiveMQJMS2TestBase {

    private final String destinationName;
    private final String destinationType;
    private final int ackMode;
    private final String messagePayload;

    public ActiveMQJMS2AckModesTest(String destinationType, int ackMode) {
        this.destinationName = "AMQ.JMS2.ACKMODE." + Integer.toString(ackMode) + destinationType.toUpperCase();
        this.destinationType = destinationType;
        this.ackMode = ackMode;
        this.messagePayload = "Test message destType: " + destinationType + " ackMode: " + Integer.toString(ackMode);
    }

    @Parameterized.Parameters(name="destinationType={0}, ackMode={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"queue", ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE },
                {"queue", ActiveMQSession.AUTO_ACKNOWLEDGE },
                {"queue", ActiveMQSession.CLIENT_ACKNOWLEDGE },
                {"queue", ActiveMQSession.DUPS_OK_ACKNOWLEDGE },
                {"queue", ActiveMQSession.SESSION_TRANSACTED },
                {"topic", ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE },
                {"topic", ActiveMQSession.AUTO_ACKNOWLEDGE },
                {"topic", ActiveMQSession.CLIENT_ACKNOWLEDGE },
                {"topic", ActiveMQSession.DUPS_OK_ACKNOWLEDGE },
                {"topic", ActiveMQSession.SESSION_TRANSACTED },
                {"temp-queue", ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE },
                {"temp-queue", ActiveMQSession.AUTO_ACKNOWLEDGE },
                {"temp-queue", ActiveMQSession.CLIENT_ACKNOWLEDGE },
                {"temp-queue", ActiveMQSession.DUPS_OK_ACKNOWLEDGE },
                {"temp-queue", ActiveMQSession.SESSION_TRANSACTED },
                {"temp-topic", ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE },
                {"temp-topic", ActiveMQSession.AUTO_ACKNOWLEDGE },
                {"temp-topic", ActiveMQSession.CLIENT_ACKNOWLEDGE },
                {"temp-topic", ActiveMQSession.DUPS_OK_ACKNOWLEDGE },
                {"temp-topic", ActiveMQSession.SESSION_TRANSACTED }
        });
    }

    @Test
    public void testAcknowledgementMode() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, ackMode)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, destinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);
            jmsContext.start();

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, "text", messagePayload);

            List<String> sentMessageIds = new LinkedList<>();
            for(int deliveryMode : Arrays.asList(DeliveryMode.NON_PERSISTENT, DeliveryMode.PERSISTENT)) {
                MessageData sendMessageData = new MessageData();
                sendMessageData.setMessage(message).setDeliveryMode(deliveryMode);
                sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));
            }

            // For session transacted ack after all messages are sent
            switch(ackMode) {
            case ActiveMQSession.SESSION_TRANSACTED: jmsContext.commit(); break;
            default: break;
            }

            Message recvMessage = null;
            Message lastRecvMessage = null;
            List<Message> recvMessages = new LinkedList<>();

            recvMessage = jmsConsumer.receive(2000l);

            while (recvMessage != null) {
                recvMessages.add(recvMessage);
                lastRecvMessage = recvMessage;
                switch(ackMode) {
                case ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE: lastRecvMessage.acknowledge(); break;
                default: break;
                }

                recvMessage = jmsConsumer.receive(500l);
            }

            assertEquals(Integer.valueOf(2), Integer.valueOf(recvMessages.size()));

            switch(ackMode) {
            case ActiveMQSession.CLIENT_ACKNOWLEDGE: lastRecvMessage.acknowledge(); break;
            case ActiveMQSession.SESSION_TRANSACTED: jmsContext.commit(); break;
            default: break;
            }
            jmsConsumer.close();

            int foundCount = 0;
            for(int validDeliveryMode : Arrays.asList(DeliveryMode.NON_PERSISTENT, DeliveryMode.PERSISTENT)) {
                for(javax.jms.Message tmpMessage : recvMessages) {
                    if(tmpMessage.getJMSDeliveryMode() == validDeliveryMode) {
                        MessageData messageData = new MessageData();
                        messageData.setMessageType("text").setMessagePayload(messagePayload).setDeliveryMode(validDeliveryMode);
                        ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, messageData);
                        foundCount++;
                    }
                }
            }
            assertEquals(Integer.valueOf(2), Integer.valueOf(foundCount));

            DestinationViewMBean destinationViewMBean = getDestinationViewMBean(destinationType, (ActiveMQDestination)destination);
            assertTrue("QueueSize = 0 expected", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return destinationViewMBean.getQueueSize() == 0l;
                }
            }, 5000l, 10l));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


}
