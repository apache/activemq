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


import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AMQ2183Test extends TestCase implements UncaughtExceptionHandler, MessageListener {
       
    private static final Log LOG = LogFactory.getLog(AMQ2183Test.class);
    private static final int maxSent = 2000;    
    private final Map<Thread, Throwable> exceptions = new ConcurrentHashMap<Thread, Throwable>();

    BrokerService master = new BrokerService();
    BrokerService slave = new BrokerService();
    URI masterUrl, slaveUrl;

    public void onException(JMSException e) {
        exceptions.put(Thread.currentThread(), e);
    }
    
    public void setUp() throws Exception {
    
        master = new BrokerService();
        slave = new BrokerService();
        
        master.setBrokerName("Master");
        master.addConnector("tcp://localhost:0");
        master.deleteAllMessages();
        master.setWaitForSlave(true);
        
        Thread t = new Thread() {
            public void run() {
                try {
                    master.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptions.put(Thread.currentThread(), e);
                }
            }
        };
        t.start();
        Thread.sleep(2000);
        masterUrl = master.getTransportConnectors().get(0).getConnectUri();
        
        slave.setBrokerName("Slave");
        slave.deleteAllMessages();
        slave.addConnector("tcp://localhost:0");
        slave.setMasterConnectorURI(masterUrl.toString());
        slave.start();
        slaveUrl = slave.getTransportConnectors().get(0).getConnectUri();
    }
    
    public void tearDown() throws Exception {
        master.stop();
        slave.stop();
        exceptions.clear();
    }
    
    class MessageCounter implements MessageListener {
        int count = 0;
        public void onMessage(Message message) {
            count++;
        }
        
        int getCount() {
            return count;
        }
    }
    
    public void testMasterSlaveBugWithStopStartConsumers() throws Exception {

        Thread.setDefaultUncaughtExceptionHandler(this);
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "failover:(" + masterUrl + ")?randomize=false");
    
        Connection connection = connectionFactory.createConnection();
        connection.start();
        final MessageCounter counterA = new MessageCounter();
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQQueue("Consumer.A.VirtualTopic.T")).setMessageListener(counterA);
       
        final MessageCounter counterB = new MessageCounter();
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQQueue("Consumer.B.VirtualTopic.T")).setMessageListener(counterB);
       
        Thread.sleep(2000);
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQTopic("VirtualTopic.T"));
        for (int i=0; i<maxSent; i++) {
            producer.send(session.createTextMessage("Hi" + i));
        }
        
        Wait.waitFor(new Condition() {
            public boolean isSatisified() throws Exception {
                return maxSent == counterA.getCount() && maxSent == counterB.getCount();
            }
        });
        assertEquals(maxSent, counterA.getCount());
        assertEquals(maxSent, counterB.getCount());
        assertTrue(exceptions.isEmpty());
    }


    public void uncaughtException(Thread t, Throwable e) {
        exceptions.put(t,e);
    }

    public void onMessage(Message message) {
        LOG.info("message received: " + message);        
    }
}
