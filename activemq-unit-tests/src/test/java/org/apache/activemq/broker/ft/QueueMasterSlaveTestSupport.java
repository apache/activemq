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
package org.apache.activemq.broker.ft;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * Test failover for Queues
 */
abstract public class QueueMasterSlaveTestSupport extends JmsTopicSendReceiveWithTwoConnectionsTest {
    private static final transient Logger LOG = LoggerFactory.getLogger(QueueMasterSlaveTestSupport.class);

    protected BrokerService master;
    protected AtomicReference<BrokerService> slave = new AtomicReference<BrokerService>();
    protected CountDownLatch slaveStarted;
    protected int inflightMessageCount;
    protected int failureCount = 50;
    protected String uriString = "failover://(tcp://localhost:62001,tcp://localhost:62002)?randomize=false&useExponentialBackOff=false";

    @Override
    protected void setUp() throws Exception {
        slaveStarted = new CountDownLatch(1);
        slave.set(null);
        setMaxTestTime(TimeUnit.MINUTES.toMillis(10));
        setAutoFail(true);
        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }
        super.messageCount = 500;
        failureCount = super.messageCount / 2;
        super.topic = isTopic();
        createMaster();
        createSlave();
        // wait for thing to connect
        Thread.sleep(1000);
        super.setUp();
    }

    protected String getSlaveXml() {
        return "org/apache/activemq/broker/ft/slave.xml";
    }

    protected String getMasterXml() {
        return "org/apache/activemq/broker/ft/master.xml";
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        master.stop();
        master.waitUntilStopped();
        slaveStarted.await(60, TimeUnit.SECONDS);
        BrokerService brokerService = slave.get();
        if( brokerService!=null ) {
            brokerService.stop();
        }
        master.stop();
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(uriString);
    }

    @Override
    protected void messageSent() throws Exception {
        if (++inflightMessageCount == failureCount) {
            Thread.sleep(1000);
            LOG.error("MASTER STOPPED!@!!!!");
            master.stop();
        }
    }

    protected boolean isTopic() {
        return false;
    }

    protected void createMaster() throws Exception {
        BrokerFactoryBean brokerFactory = new BrokerFactoryBean(new ClassPathResource(getMasterXml()));
        brokerFactory.afterPropertiesSet();
        master = brokerFactory.getBroker();
        master.start();
    }

    protected void createSlave() throws Exception {
        BrokerFactoryBean brokerFactory = new BrokerFactoryBean(new ClassPathResource(getSlaveXml()));
        brokerFactory.afterPropertiesSet();
        BrokerService broker = brokerFactory.getBroker();
        broker.start();
        slave.set(broker);
        slaveStarted.countDown();
    }

    public void testVirtualTopicFailover() throws Exception {

        MessageConsumer qConsumer = session.createConsumer(new ActiveMQQueue("Consumer.A.VirtualTopic.TA1"));
        assertNull("No message there yet", qConsumer.receive(1000));
        qConsumer.close();
        assertTrue("master is indeed the master", !master.isSlave());
        master.stop();
        assertTrue("slave started", slaveStarted.await(60, TimeUnit.SECONDS));
        assertTrue(!slave.get().isSlave());

        LOG.info("Sending post failover message to VT");

        final String text = "ForUWhenSlaveKicksIn";
        producer.send(new ActiveMQTopic("VirtualTopic.TA1"), session.createTextMessage(text));

        // dest must survive failover - consumer created after send
        qConsumer = session.createConsumer(new ActiveMQQueue("Consumer.A.VirtualTopic.TA1"));

        javax.jms.Message message = qConsumer.receive(10000);
        assertNotNull("Get message after failover", message);
        assertEquals("correct message", text, ((TextMessage)message).getText());
    }

    public void testAdvisory() throws Exception {
        final MessageConsumer advConsumer = session.createConsumer(AdvisorySupport.getMasterBrokerAdvisoryTopic());
        final Message[] advisoryMessage = new Message[1];
        advisoryMessage[0] = advConsumer.receive(5000);
        LOG.info("received " + advisoryMessage[0]);
        assertNotNull("Didn't received advisory", advisoryMessage[0]);

        master.stop();
        assertTrue("slave started", slaveStarted.await(60, TimeUnit.SECONDS));
        LOG.info("slave started");
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                advisoryMessage[0] = advConsumer.receive(500);
                return advisoryMessage[0] != null;
            }
        });
        LOG.info("received " + advisoryMessage[0]);
        assertNotNull("Didn't received advisory", advisoryMessage[0]);

    }
}
