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
package org.apache.activegroups;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

public class GroupMessageTest extends TestCase {
    protected BrokerService broker;
    protected String bindAddress = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL;

    public void testGroupBroadcast() throws Exception {
        final int number = 10;
        final AtomicInteger count = new AtomicInteger();
        List<Group> groups = new ArrayList<Group>();
        ConnectionFactory factory = createConnectionFactory();
        for (int i = 0; i < number; i++) {
            Connection connection = factory.createConnection();
            Group group = new Group(connection, "group" + i);            
            group.setMinimumGroupSize(i+1);
            group.start();
            groups.add(group);
            group.addGroupMessageListener(new GroupMessageListener() {
                public void messageDelivered(Member sender, String replyId,
                        Object message) {
                    synchronized (count) {
                        if (count.incrementAndGet() == number) {
                            count.notifyAll();
                        }
                    }
                }
            });
        }
        groups.get(0).broadcastMessage("hello");
        synchronized (count) {
            if (count.get() < number) {
                count.wait(5000);
            }
        }
        assertEquals(number, count.get());
        for (Group map : groups) {
            map.stop();
        }
    }

    public void testsendMessage() throws Exception {
        final int number = 10;
        final AtomicInteger count = new AtomicInteger();
        List<Group> groups = new ArrayList<Group>();
        ConnectionFactory factory = createConnectionFactory();
        for (int i = 0; i < number; i++) {
            Connection connection = factory.createConnection();
            Group group = new Group(connection, "group" + i);
            group.setMinimumGroupSize(i+1);
            group.start();
            groups.add(group);
            group.addGroupMessageListener(new GroupMessageListener() {
                public void messageDelivered(Member sender, String replyId,
                        Object message) {
                    synchronized (count) {
                        count.incrementAndGet();
                        count.notifyAll();
                    }
                }
            });
        }
        groups.get(0).sendMessage("hello");
        synchronized (count) {
            if (count.get() == 0) {
                count.wait(5000);
            }
        }
        // wait a while to check that only one got it
        Thread.sleep(2000);
        assertEquals(1, count.get());
        for (Group map : groups) {
            map.stop();
        }
    }

    public void testSendToSingleMember() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection connection1 = factory.createConnection();
        Connection connection2 = factory.createConnection();
        Group group1 = new Group(connection1, "group1");
        final AtomicBoolean called = new AtomicBoolean();
        group1.addGroupMessageListener(new GroupMessageListener() {
            public void messageDelivered(Member sender, String replyId,
                    Object message) {
                synchronized (called) {
                    called.set(true);
                    called.notifyAll();
                }
            }
        });
        group1.start();
        Group group2 = new Group(connection2, "group2");
        group2.setMinimumGroupSize(2);
        group2.start();
        Member member1 = group2.getMemberByName("group1");
        group2.sendMessage(member1, "hello");
        synchronized (called) {
            if (!called.get()) {
                called.wait(5000);
            }
        }
        assertTrue(called.get());
        group1.stop();
        group2.stop();
    }

    public void testSendRequestReply() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection connection1 = factory.createConnection();
        Connection connection2 = factory.createConnection();
        final int number = 1000;
        final AtomicInteger requestCount = new AtomicInteger();
        final AtomicInteger replyCount = new AtomicInteger();
        final List<String> requests = new ArrayList<String>();
        final List<String> replies = new ArrayList<String>();
        for (int i = 0; i < number; i++) {
            requests.add("request" + i);
            replies.add("reply" + i);
        }
        final Group group1 = new Group(connection1, "group1");
        final AtomicBoolean finished = new AtomicBoolean();
        group1.addGroupMessageListener(new GroupMessageListener() {
            public void messageDelivered(Member sender, String replyId,
                    Object message) {
                if (!replies.isEmpty()) {
                    String reply = replies.remove(0);
                    try {
                        group1.sendMessageResponse(sender, replyId, reply);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                } 
            }
        });
        group1.start();
        final Group group2 = new Group(connection2, "group2");
        group2.setMinimumGroupSize(2);
        group2.addGroupMessageListener(new GroupMessageListener() {
            public void messageDelivered(Member sender, String replyId,
                    Object message) {
                if (!requests.isEmpty()) {
                    String request = requests.remove(0);
                    try {
                        group2.sendMessage(sender, request);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }else {
                    synchronized (finished) {
                        finished.set(true);
                        finished.notifyAll();
                    }
                }
            }
        });
        group2.start();
        Member member1 = group2.getMemberByName("group1");
        group2.sendMessage(member1, requests.remove(0));
        synchronized (finished) {
            if (!finished.get()) {
                finished.wait(10000);
            }
        }
        assertTrue(finished.get());
        group1.stop();
        group2.stop();
    }

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory()
            throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                ActiveMQConnection.DEFAULT_BROKER_URL);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setPersistent(false);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}
