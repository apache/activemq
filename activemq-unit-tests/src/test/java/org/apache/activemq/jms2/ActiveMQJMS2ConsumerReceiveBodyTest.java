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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.Session;

@RunWith(value = Parameterized.class)
public class ActiveMQJMS2ConsumerReceiveBodyTest extends ActiveMQJMS2TestBase {

    private final String destinationName;
    private final String destinationType;
    private final int ackMode;
    private final String messagePayload;
    private final String messageType;
    private final Class<?> bodyTypeClass;

    public ActiveMQJMS2ConsumerReceiveBodyTest(String destinationType, String messageType, Class<?> bodyTypeClass) {
        this.destinationName = "AMQ.JMS2.RECVBODY." + messageType + "." + destinationType.toUpperCase();
        this.destinationType = destinationType;
        this.ackMode = Session.AUTO_ACKNOWLEDGE;
        this.messageType = messageType;
        this.bodyTypeClass = bodyTypeClass;
        this.messagePayload = "Test message destType: " + destinationType + " ackMode: " + Integer.toString(ackMode);
    }

    @Parameterized.Parameters(name="destinationType={0}, messageType={1}, bodyType={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"queue", "bytes", byte[].class},
                {"queue", "map", Map.class},
                {"queue", "object", Object.class},
                {"queue", "stream", Object.class},
                {"queue", "text", String.class},
                {"topic", "bytes", byte[].class},
                {"topic", "map", Map.class},
                {"topic", "object", Object.class},
                {"topic", "stream", Object.class},
                {"topic", "text", String.class},
                {"temp-queue", "bytes", byte[].class},
                {"temp-queue", "map", Map.class},
                {"temp-queue", "object", Object.class},
                {"temp-queue", "stream", Object.class},
                {"temp-queue", "text", String.class},
                {"temp-topic", "bytes", byte[].class},
                {"temp-topic", "map", Map.class},
                {"temp-topic", "object", Object.class},
                {"temp-topic", "stream", Object.class},
                {"temp-topic", "text", String.class},
        });
    }

    @Test
    public void testReceiveBody() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, ackMode)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, destinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);
            jmsContext.start();

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);

            List<String> sentMessageIds = new LinkedList<>();
            for(int deliveryMode : Arrays.asList(DeliveryMode.NON_PERSISTENT, DeliveryMode.PERSISTENT)) {
                MessageData sendMessageData = new MessageData();
                sendMessageData.setMessage(message).setDeliveryMode(deliveryMode);
                sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));
            }

            boolean formatExceptionCaught = false;
            long expectedTopicSize = 0l;
            long expectedQueueSize = (messageType == "stream" ? 2l : 0l);
            try{
                Object recvMessageBody = null;
            
                List<Object> recvMessageBodies = new LinkedList<>();

                recvMessageBody = jmsConsumer.receiveBody(bodyTypeClass, 2000l);
    
                while (recvMessageBody != null) {
                    recvMessageBodies.add(recvMessageBody);
                    recvMessageBody = jmsConsumer.receiveBody(bodyTypeClass, 500l);
                }
    
                assertEquals(Integer.valueOf(2), Integer.valueOf(recvMessageBodies.size()));
                jmsConsumer.close();
    
                int foundCount = 0;
                for(Object tmpMessageBody : recvMessageBodies) {
                    assertTrue(bodyTypeClass.isAssignableFrom(tmpMessageBody.getClass()));
                    foundCount++;
                }
                assertEquals(Integer.valueOf(2), Integer.valueOf(foundCount));
            } catch (MessageFormatRuntimeException e) {
                formatExceptionCaught = true;
            } finally {
                if("stream".equals(messageType)) {
                    assertTrue(formatExceptionCaught);
                } else {
                    assertFalse(formatExceptionCaught);
                }
            }

            ActiveMQDestination activemqDestination = (ActiveMQDestination)destination;
            DestinationViewMBean destinationViewMBean = getDestinationViewMBean(destinationType, activemqDestination);
            assertTrue("QueueSize expected: " + expectedQueueSize, Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    if(activemqDestination.isQueue()) {
                        return destinationViewMBean.getQueueSize() == expectedQueueSize;
                    } else {
                        return destinationViewMBean.getQueueSize() == expectedTopicSize;
                    }
                }
            }, 5000l, 10l));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
