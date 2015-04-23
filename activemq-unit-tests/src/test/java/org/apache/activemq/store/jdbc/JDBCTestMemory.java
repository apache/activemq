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
package org.apache.activemq.store.jdbc;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.Ignore;


public class JDBCTestMemory extends TestCase {

    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
    Connection conn;
    Session sess;
    Destination dest;
    
    BrokerService broker;
    
    protected void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    protected void tearDown() throws Exception {
        broker.stop();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.deleteAllMessages();
        broker.setPersistenceAdapter(jdbc);
        broker.addConnector("tcp://0.0.0.0:61616");
        return broker;
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        broker.setPersistenceAdapter(jdbc);
        broker.addConnector("tcp://0.0.0.0:61616");
        return broker;
    }
    
    public void init() throws Exception {
        conn = factory.createConnection();
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        dest = sess.createQueue("test");        
    }

    @Ignore("requires human input to terminate!")
    public void testRecovery() throws Exception {
        init();
        MessageProducer producer = sess.createProducer(dest);
        for (int i = 0; i < 1000; i++) {
            producer.send(sess.createTextMessage("test"));
        }
        producer.close();
        sess.close();
        conn.close();
        
        broker.stop();
        broker.waitUntilStopped();
        broker = createRestartedBroker();
        broker.start();
        broker.waitUntilStarted();
        
        init();
        
        for (int i = 0; i < 10; i++) {
            new Thread("Producer " + i) {

                public void run() {
                    try {
                        MessageProducer producer = sess.createProducer(dest);
                        for (int i = 0; i < 15000; i++) {
                            producer.send(sess.createTextMessage("test"));
                            if (i % 100 == 0) {
                                System.out.println(getName() + " sent message " + i);
                            }
                        }
                        producer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                
            }.start();
            
            new Thread("Consumer " + i) {

                public void run() {
                    try {
                        MessageConsumer consumer = sess.createConsumer(dest);
                        for (int i = 0; i < 15000; i++) {
                            consumer.receive(2000);
                            if (i % 100 == 0) {
                                System.out.println(getName() + " received message " + i);
                            }
                        }
                        consumer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                
            }.start();
        }
        
        // Check out JConsole
        System.in.read();
        sess.close();
        conn.close();
    }
    
}
