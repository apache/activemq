/*
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
package org.apache.activemq.xbean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.SharedTopicRegion;
import org.junit.After;
import org.junit.Test;

/**
 * Shows how to wire a {@link SharedTopicBrokerService} from Spring XML via the
 * {@code <sharedTopicBrokerService>} XBean element, and verifies the element
 * actually produces a shared-topic-capable broker rather than a plain one.
 *
 * @see "src/test/resources/spring/shared-topic-broker.xml"
 */
public class SharedTopicBrokerServiceXBeanTest {

    private BrokerService broker;

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    @Test
    public void testXBeanElementCreatesSharedTopicBrokerService() throws Exception {
        broker = createBroker();

        assertTrue("<sharedTopicBrokerService> must yield a SharedTopicBrokerService, not a "
                + "plain BrokerService; got " + broker.getClass().getName(),
                broker instanceof SharedTopicBrokerService);
        assertEquals("sharedTopicXBeanBroker", broker.getBrokerName());
    }

    /**
     * The whole point of the element: without v13 the shared flag is lost on
     * restart, so a broker wired this way must not be left at the default
     * store version.
     */
    @Test
    public void testStoreOpenWireVersionIsThirteen() throws Exception {
        broker = createBroker();

        assertEquals("shared subs require OpenWire v13 for KahaDB persistence",
                13, broker.getStoreOpenWireVersion());
        assertTrue("v13 must be an upgrade over the plain broker default",
                13 > new BrokerService().getStoreOpenWireVersion());
    }

    @Test
    public void testXmlAttributeIsAppliedToTheBroker() throws Exception {
        broker = createBroker();

        assertTrue("topicSubscriptionConversionEnabled=\"true\" in the XML must reach the bean",
                ((SharedTopicBrokerService) broker).isTopicSubscriptionConversionEnabled());
    }

    /**
     * Attributes inherited from {@code <broker>} must still work on the
     * subclass element, since XBean maps setters from the whole hierarchy.
     */
    @Test
    public void testInheritedBrokerAttributesStillApply() throws Exception {
        broker = createBroker();

        assertEquals(false, broker.isUseJmx());
        assertEquals(false, broker.isPersistent());
        assertNotNull("transportConnector from the XML should be present",
                broker.getTransportConnectorByScheme("tcp"));
    }

    @Test
    public void testSharedTopicRegionIsInstalled() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        assertTrue("broker must install SharedTopicRegion as its topic region; got "
                + regionBroker.getTopicRegion().getClass().getName(),
                regionBroker.getTopicRegion() instanceof SharedTopicRegion);
    }

    private BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker("xbean:spring/shared-topic-broker.xml");
    }
}
