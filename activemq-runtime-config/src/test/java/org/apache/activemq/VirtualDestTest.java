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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;
import javax.jms.*;
import javax.jms.Message;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;


import static org.junit.Assert.*;

public class VirtualDestTest {

    public static final Logger LOG = LoggerFactory.getLogger(VirtualDestTest.class);
    public static final int SLEEP = 4; // seconds
    String configurationSeed = "virtualDestTest";
    BrokerService brokerService;

    public void startBroker(String configFileName) throws Exception {
        brokerService = createBroker(configFileName);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public BrokerService createBroker(String configFileName) throws Exception {
        brokerService = new BrokerService();
        return BrokerFactory.createBroker("xbean:org/apache/activemq/" + configFileName + ".xml");
    }

    @After
    public void stopBroker() throws Exception {
        brokerService.stop();
    }

    @Test
    public void testNew() throws Exception {
        final String brokerConfig = configurationSeed + "-no-vd-broker";
        applyNewConfig(brokerConfig, NetworkConnectorTest.EMPTY_UPDATABLE_CONFIG);
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        // default config has support for VirtualTopic.>
        DestinationInterceptor[] interceptors  = brokerService.getDestinationInterceptors();
        assertEquals("one interceptor", 1, interceptors.length);
        assertTrue("it is virtual topic interceptor", interceptors[0] instanceof VirtualDestinationInterceptor);

        VirtualDestinationInterceptor defaultValue = (VirtualDestinationInterceptor) interceptors[0];
        assertEquals("default names in place", "VirtualTopic.>",
                defaultValue.getVirtualDestinations()[0].getVirtualDestination().getPhysicalName());

        exerciseVirtualTopic("VirtualTopic.Default");

        applyNewConfig(brokerConfig, configurationSeed + "-one-vd", SLEEP);

        assertEquals("one interceptor", 1, interceptors.length);
        assertTrue("it is virtual topic interceptor", interceptors[0] instanceof VirtualDestinationInterceptor);

        // update will happen on addDestination
        exerciseVirtualTopic("A.Default");

        VirtualDestinationInterceptor newValue = (VirtualDestinationInterceptor) interceptors[0];
        assertEquals("new names in place", "A.>",
                defaultValue.getVirtualDestinations()[0].getVirtualDestination().getPhysicalName());

        // apply again - ensure no change
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd");
        assertSame("same instance", newValue, (((VirtualDestinationInterceptor) brokerService.getDestinationInterceptors()[0])));
    }


    @Test
    public void testNewNoDefaultVirtualTopicSupport() throws Exception {
        final String brokerConfig = configurationSeed + "-no-vd-broker";
        applyNewConfig(brokerConfig, NetworkConnectorTest.EMPTY_UPDATABLE_CONFIG);
        brokerService = createBroker(brokerConfig);
        brokerService.setUseVirtualTopics(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        assertTrue("broker alive", brokerService.isStarted());

        DestinationInterceptor[] interceptors  = brokerService.getDestinationInterceptors();
        assertEquals("one interceptor", 0, interceptors.length);

        applyNewConfig(brokerConfig, configurationSeed + "-one-vd", SLEEP);

        // update will happen on addDestination
        exerciseVirtualTopic("A.Default");

        interceptors  = brokerService.getDestinationInterceptors();
        assertEquals("one interceptor", 1, interceptors.length);
        assertTrue("it is virtual topic interceptor", interceptors[0] instanceof VirtualDestinationInterceptor);

        VirtualDestinationInterceptor newValue = (VirtualDestinationInterceptor) interceptors[0];

        // apply again - ensure no change
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd");
        assertSame("same instance", newValue, (((VirtualDestinationInterceptor) brokerService.getDestinationInterceptors()[0])));
    }

    @Test
    public void testNewWithMirrorQueueSupport() throws Exception {
        final String brokerConfig = configurationSeed + "-no-vd-broker";
        applyNewConfig(brokerConfig, NetworkConnectorTest.EMPTY_UPDATABLE_CONFIG);
        brokerService = createBroker(brokerConfig);
        brokerService.setUseMirroredQueues(true);
        brokerService.start();
        brokerService.waitUntilStarted();

        assertTrue("broker alive", brokerService.isStarted());

        DestinationInterceptor[] interceptors  = brokerService.getDestinationInterceptors();
        assertEquals("expected interceptor", 2, interceptors.length);

        applyNewConfig(brokerConfig, configurationSeed + "-one-vd", SLEEP);

        // update will happen on addDestination
        exerciseVirtualTopic("A.Default");

        interceptors  = brokerService.getDestinationInterceptors();
        assertEquals("expected interceptor", 2, interceptors.length);
        assertTrue("it is virtual topic interceptor", interceptors[0] instanceof VirtualDestinationInterceptor);

        VirtualDestinationInterceptor newValue = (VirtualDestinationInterceptor) interceptors[0];

        // apply again - ensure no change
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd");
        assertSame("same instance", newValue, (((VirtualDestinationInterceptor) brokerService.getDestinationInterceptors()[0])));
    }

    @Test
    public void testRemove() throws Exception {
        final String brokerConfig = configurationSeed + "-one-vd-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        DestinationInterceptor[] interceptors  = brokerService.getDestinationInterceptors();
        assertEquals("one interceptor", 1, interceptors.length);
        assertTrue("it is virtual topic interceptor", interceptors[0] instanceof VirtualDestinationInterceptor);

        VirtualDestinationInterceptor defaultValue = (VirtualDestinationInterceptor) interceptors[0];
        assertEquals("configured names in place", "A.>",
                defaultValue.getVirtualDestinations()[0].getVirtualDestination().getPhysicalName());

        exerciseVirtualTopic("A.Default");

        applyNewConfig(brokerConfig, NetworkConnectorTest.EMPTY_UPDATABLE_CONFIG, SLEEP);

        // update will happen on addDestination
        forceAddDestination("AnyDest");

        assertTrue("getDestinationInterceptors empty on time", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() {
                return 0 == brokerService.getDestinationInterceptors().length;
            }
        }));

        // reverse the remove, add again
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd", SLEEP);

        // update will happen on addDestination
        exerciseVirtualTopic("A.NewOne");

        interceptors  = brokerService.getDestinationInterceptors();
        assertEquals("expected interceptor", 1, interceptors.length);
        assertTrue("it is virtual topic interceptor", interceptors[0] instanceof VirtualDestinationInterceptor);
    }

    @Test
    public void testMod() throws Exception {
        final String brokerConfig = configurationSeed + "-one-vd-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        assertEquals("one interceptor", 1, brokerService.getDestinationInterceptors().length);
        exerciseVirtualTopic("A.Default");

        applyNewConfig(brokerConfig, configurationSeed + "-mod-one-vd", SLEEP);
        exerciseVirtualTopic("B.Default");

        assertEquals("still one interceptor", 1, brokerService.getDestinationInterceptors().length);
    }


    @Test
    public void testModWithMirroredQueue() throws Exception {
        final String brokerConfig = configurationSeed + "-one-vd-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd");
        brokerService = createBroker(brokerConfig);
        brokerService.setUseMirroredQueues(true);
        brokerService.start();
        brokerService.waitUntilStarted();

        assertEquals("one interceptor", 1, brokerService.getDestinationInterceptors().length);
        exerciseVirtualTopic("A.Default");

        applyNewConfig(brokerConfig, configurationSeed + "-mod-one-vd", SLEEP);
        exerciseVirtualTopic("B.Default");

        assertEquals("still one interceptor", 1, brokerService.getDestinationInterceptors().length);
    }

    private void forceAddDestination(String dest) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createConsumer(session.createQueue("Consumer.A." + dest));
        connection.close();
    }

    private void exerciseVirtualTopic(String topic) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(session.createQueue("Consumer.A." + topic));
        MessageProducer producer = session.createProducer(session.createTopic(topic));
        final String body = "To vt:" + topic;
        producer.send(session.createTextMessage(body));

        Message message = null;
        for (int i=0; i<5 && message == null; i++) {
            message = consumer.receive(1000);
        }
        assertNotNull("got message", message);
        assertEquals("got expected message", body, ((TextMessage) message).getText());
        connection.close();
    }

    private void applyNewConfig(String configName, String newConfigName) throws Exception {
        applyNewConfig(configName, newConfigName, 0l);
    }

    private void applyNewConfig(String configName, String newConfigName, long sleep) throws Exception {
        Resource resource = Utils.resourceFromString("org/apache/activemq");
        FileOutputStream current = new FileOutputStream(new File(resource.getFile(), configName + ".xml"));
        FileInputStream modifications = new FileInputStream(new File(resource.getFile(), newConfigName + ".xml"));
        modifications.getChannel().transferTo(0, Long.MAX_VALUE, current.getChannel());
        current.flush();
        LOG.info("Updated: " + current.getChannel());

        if (sleep > 0) {
            // wait for mods to kick in
            TimeUnit.SECONDS.sleep(sleep);
        }
    }
}
