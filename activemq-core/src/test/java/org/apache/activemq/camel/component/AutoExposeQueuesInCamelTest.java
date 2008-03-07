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
package org.apache.activemq.camel.component;

import java.util.List;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.BrowsableEndpoint;
import org.apache.camel.util.CamelContextHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Shows that we can see the queues inside ActiveMQ via Camel
 * by enabling the {@link ActiveMQComponent#setExposeAllQueues(boolean)} flag
 *
 * @version $Revision$
 */
public class AutoExposeQueuesInCamelTest extends EmbeddedBrokerTestSupport {
    private static final transient Log LOG = LogFactory.getLog(AutoExposeQueuesInCamelTest.class);

    protected ActiveMQQueue sampleQueue = new ActiveMQQueue("foo.bar");
    protected ActiveMQTopic sampleTopic = new ActiveMQTopic("cheese");

    protected CamelContext camelContext = new DefaultCamelContext();
    protected CamelTemplate template;

    public void testWorks() throws Exception {
        Thread.sleep(2000);
        LOG.debug("Looking for endpoints...");
        List<BrowsableEndpoint> endpoints = CamelContextHelper.getSingletonEndpoints(camelContext, BrowsableEndpoint.class);
        for (BrowsableEndpoint endpoint : endpoints) {
            LOG.debug("Endpoint: " + endpoint);
        }
        assertEquals("Should have found an endpoint: "+ endpoints, 1, endpoints.size());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // lets configure the ActiveMQ component for Camel
        ActiveMQComponent component = new ActiveMQComponent();
        component.setBrokerURL(bindAddress);
        component.setExposeAllQueues(true);

        camelContext.addComponent("activemq", component);
        camelContext.start();
    }

    @Override
    protected void tearDown() throws Exception {
        camelContext.stop();
        super.tearDown();
    }


    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setDestinations(new ActiveMQDestination[]{
                sampleQueue,
                sampleTopic
        });
        return broker;
    }

}