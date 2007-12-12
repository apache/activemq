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
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * Test failover for Queues
 */
public class QueueMasterSlaveTest extends JmsTopicSendReceiveWithTwoConnectionsTest {
    private static final transient Log LOG = LogFactory.getLog(QueueMasterSlaveTest.class);

    protected BrokerService master;
    protected BrokerService slave;
    protected int inflightMessageCount;
    protected int failureCount = 50;
    protected String uriString = "failover://(tcp://localhost:62001,tcp://localhost:62002)?randomize=false";

    protected void setUp() throws Exception {
        messageCount = 10000;
        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }
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

    protected void tearDown() throws Exception {
        super.tearDown();
        slave.stop();
        master.stop();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(uriString);
    }

    protected void messageSent() throws Exception {
        if (++inflightMessageCount >= failureCount) {
            inflightMessageCount = 0;
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
        slave = brokerFactory.getBroker();
        slave.start();
    }
}
