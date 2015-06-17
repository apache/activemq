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
package org.apache.activemq.broker.region.cursors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.test.TestSupport;

/**
 * This test shows that a null pointer exception will not occur when unsubscribing from a
 * subscription while a producer is sending messages rapidly to the topic.  A null pointer
 * exception was occurring in the setLastCachedId method of AbstractMessageCursor due to
 * a race condition.  If this test is run before the patch that is applied in this commit
 * on AbstractStoreCusor, it will consistently fail with a NPE.
 *
 */
public class AbstractStoreCursorNpeTest extends TestSupport {

    protected Connection connection;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Topic destination;


    public void testSetLastCachedIdNPE() throws Exception {
        connection = createConnection();
        connection.setClientID("clientId");
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createTopic("test.topic");
        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);


        Connection durableCon = createConnection();
        durableCon.setClientID("testCons");
        durableCon.start();
        final Session durSession = durableCon.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //In a new thread rapidly subscribe and unsubscribe to a durable
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try{
                    //Repeatedly create a durable subscription and then unsubscribe which used to
                    //cause a NPE while messages were sending
                    while(true) {
                        MessageConsumer cons = durSession.createDurableSubscriber(durSession.createTopic("test.topic"), "sub1");
                        Thread.sleep(100);
                        cons.close();
                        durSession.unsubscribe("sub1");
                    }
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        });

        TextMessage myMessage = new ActiveMQTextMessage();
        myMessage.setText("test");
        //Make sure that we can send a bunch of messages without a NPE
        //This would fail if the patch is not applied
        for (int i = 0; i < 10000; i++) {
            producer.send(myMessage);
        }
    }
}
