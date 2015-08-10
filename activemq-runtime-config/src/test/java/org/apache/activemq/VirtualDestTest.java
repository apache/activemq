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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.util.Wait;
import org.junit.Test;

public class VirtualDestTest extends AbstractVirtualDestTest {

    String configurationSeed = "virtualDestTest";

    @Test
    public void testNew() throws Exception {
        final String brokerConfig = configurationSeed + "-new-no-vd-broker";
        applyNewConfig(brokerConfig, RuntimeConfigTestSupport.EMPTY_UPDATABLE_CONFIG);
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
        assertSame("same instance", newValue, brokerService.getDestinationInterceptors()[0]);
    }

    @Test
    public void testNewComposite() throws Exception {
        final String brokerConfig = configurationSeed + "-new-composite-vd-broker";
        applyNewConfig(brokerConfig, RuntimeConfigTestSupport.EMPTY_UPDATABLE_CONFIG);
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        applyNewConfig(brokerConfig, configurationSeed + "-add-composite-vd", SLEEP);

        exerciseCompositeQueue("VirtualDestination.CompositeQueue", "VirtualDestination.QueueConsumer");
    }

    @Test
    public void testModComposite() throws Exception {
        final String brokerConfig = configurationSeed + "-mod-composite-vd-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-add-composite-vd");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());
        exerciseCompositeQueue("VirtualDestination.CompositeQueue", "VirtualDestination.QueueConsumer");

        applyNewConfig(brokerConfig, configurationSeed + "-mod-composite-vd", SLEEP);
        exerciseCompositeQueue("VirtualDestination.CompositeQueue", "VirtualDestination.QueueConsumer");

        exerciseCompositeQueue("VirtualDestination.CompositeQueue", "VirtualDestination.CompositeQueue");
    }

    @Test
    public void testNewNoDefaultVirtualTopicSupport() throws Exception {
        final String brokerConfig = configurationSeed + "-no-vd-vt-broker";
        applyNewConfig(brokerConfig, RuntimeConfigTestSupport.EMPTY_UPDATABLE_CONFIG);
        brokerService = createBroker(brokerConfig);
        brokerService.setUseVirtualTopics(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        TimeUnit.SECONDS.sleep(SLEEP);

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
        assertSame("same instance", newValue, brokerService.getDestinationInterceptors()[0]);
    }

    @Test
    public void testNewWithMirrorQueueSupport() throws Exception {
        final String brokerConfig = configurationSeed + "-no-vd-mq-broker";
        applyNewConfig(brokerConfig, RuntimeConfigTestSupport.EMPTY_UPDATABLE_CONFIG);
        brokerService = createBroker(brokerConfig);
        brokerService.setUseMirroredQueues(true);
        brokerService.start();
        brokerService.waitUntilStarted();

        TimeUnit.SECONDS.sleep(SLEEP);

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
        assertSame("same instance", newValue, brokerService.getDestinationInterceptors()[0]);
    }

    @Test
    public void testRemove() throws Exception {
        final String brokerConfig = configurationSeed + "-one-vd-rm-broker";
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

        applyNewConfig(brokerConfig, RuntimeConfigTestSupport.EMPTY_UPDATABLE_CONFIG, SLEEP);

        // update will happen on addDestination
        forceAddDestination("AnyDest");

        assertTrue("getDestinationInterceptors empty on time", Wait.waitFor(new Wait.Condition() {
            @Override
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
        final String brokerConfig = configurationSeed + "-one-vd-mod-broker";
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
        final String brokerConfig = configurationSeed + "-one-vd-mq-mod-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-one-vd");
        brokerService = createBroker(brokerConfig);
        brokerService.setUseMirroredQueues(true);
        brokerService.start();
        brokerService.waitUntilStarted();

        TimeUnit.SECONDS.sleep(SLEEP);

        assertEquals("one interceptor", 1, brokerService.getDestinationInterceptors().length);
        exerciseVirtualTopic("A.Default");

        applyNewConfig(brokerConfig, configurationSeed + "-mod-one-vd", SLEEP);
        exerciseVirtualTopic("B.Default");

        assertEquals("still one interceptor", 1, brokerService.getDestinationInterceptors().length);
    }

    @Test
    public void testNewFilteredComposite() throws Exception {
        final String brokerConfig = configurationSeed + "-new-filtered-composite-vd-broker";
        applyNewConfig(brokerConfig, RuntimeConfigTestSupport.EMPTY_UPDATABLE_CONFIG);
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        applyNewConfig(brokerConfig, configurationSeed + "-add-filtered-composite-vd", SLEEP);

        exerciseFilteredCompositeQueue("VirtualDestination.FilteredCompositeQueue", "VirtualDestination.QueueConsumer", "yes");
    }

    @Test
    public void testModFilteredComposite() throws Exception {
        final String brokerConfig = configurationSeed + "-mod-filtered-composite-vd-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-add-filtered-composite-vd");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());
        exerciseFilteredCompositeQueue("VirtualDestination.FilteredCompositeQueue", "VirtualDestination.QueueConsumer", "yes");

        applyNewConfig(brokerConfig, configurationSeed + "-mod-filtered-composite-vd", SLEEP);
        exerciseFilteredCompositeQueue("VirtualDestination.FilteredCompositeQueue", "VirtualDestination.QueueConsumer", "no");
        exerciseFilteredCompositeQueue("VirtualDestination.FilteredCompositeQueue", "VirtualDestination.QueueConsumer", "no");
    }




}
