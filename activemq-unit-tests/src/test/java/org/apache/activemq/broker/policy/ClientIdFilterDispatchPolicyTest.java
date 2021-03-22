/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.policy;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.*;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import javax.jms.*;
import java.net.URI;

import static org.junit.Assert.assertEquals;

@RunWith(BlockJUnit4ClassRunner.class)
public class ClientIdFilterDispatchPolicyTest {

    @Test
    public void testClientIdFilter() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false&useJmx=true"));;

        PolicyEntry policy = new PolicyEntry();
        policy.setDispatchPolicy(new ClientIdFilterDispatchPolicy());
        //policy.setSubscriptionRecoveryPolicy(new FixedCountSubscriptionRecoveryPolicy());
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);
        broker.start();

        // test dispacth
        String topic = "test"+ClientIdFilterDispatchPolicy.PTP_SUFFIX;
        long timeout = 5000L;
        final Result r = new Result();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        // 1.consumer1
        Connection connection1 = cf.createConnection();
        connection1.setClientID("test1");
        connection1.start();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(new ActiveMQTopic(topic));
        consumer1.setMessageListener(
                new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        try {
                            System.out.println(message.getStringProperty(ClientIdFilterDispatchPolicy.PTP_CLIENTID));
                            String clientId = message.getStringProperty(ClientIdFilterDispatchPolicy.PTP_CLIENTID);
                            //assertEquals("test1", clientId);
                            r.test1 = clientId;
                            r.count++;
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                }
        );

        // 2.consumer2
        Connection connection2 = cf.createConnection();
        connection2.setClientID("test2");
        connection2.start();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(new ActiveMQTopic(topic));
        consumer2.setMessageListener(
                new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        try {
                            System.out.println(message.getStringProperty(ClientIdFilterDispatchPolicy.PTP_CLIENTID));
                            String clientId = message.getStringProperty(ClientIdFilterDispatchPolicy.PTP_CLIENTID);
                            //assertEquals("test2", clientId);
                            r.test2 = clientId;
                            r.count++;
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                }
        );

        // 3.producer
        Message m1 = session1.createTextMessage("test1");
        m1.setStringProperty(ClientIdFilterDispatchPolicy.PTP_CLIENTID, "test1");
        Message m2 = session1.createTextMessage("test2");
        m2.setStringProperty(ClientIdFilterDispatchPolicy.PTP_CLIENTID, "test2");
        Message m3 = session1.createTextMessage("test3");
        m3.setStringProperty(ClientIdFilterDispatchPolicy.PTP_CLIENTID, "test3");

        MessageProducer producer = session1.createProducer(new ActiveMQTopic(topic));
        producer.send(m1);
        producer.send(m2);
        producer.send(m3);

        long time = 0L;

        while( r.count < 2 && time < timeout) {
            time += 50L;
            Thread.sleep(50L);
        }
        System.out.println(time);
        assertEquals(2,r.count);
        assertEquals("test1",r.test1);
        assertEquals("test2",r.test2);

        producer.close();
        session1.close();
        connection1.stop();
        session2.close();;
        connection2.close();
        broker.stop();
    }

    public static class Result{
        int count;
        public String test1;
        public String test2;
    }
}
