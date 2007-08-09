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
package org.apache.activemq.broker;

import java.net.URI;
import java.util.Set;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.xbean.XBeanBrokerFactory;

/**
 * 
 * @version $Revision$
 */
public class CreateDestinationsOnStartupViaXBeanTest extends EmbeddedBrokerTestSupport {

    public void testNewDestinationsAreCreatedOnStartup() throws Exception {
        assertQueueCreated("FOO.BAR", true);
        assertQueueCreated("FOO.DoesNotExist", false);
        
        assertTopicCreated("SOME.TOPIC", true);
        assertTopicCreated("FOO.DoesNotExist", false);
    }

    protected void assertQueueCreated(String name, boolean expected) throws Exception {
        assertDestinationCreated(new ActiveMQQueue(name), expected);
    }
    
    protected void assertTopicCreated(String name, boolean expected) throws Exception {
        assertDestinationCreated(new ActiveMQTopic(name), expected);
    }

    protected void assertDestinationCreated(ActiveMQDestination destination, boolean expected) throws Exception {
        Set answer = broker.getBroker().getDestinations(destination);
        int size = expected ? 1 : 0;
        assertEquals("Could not find destination: " + destination + ". Size of found destinations: " + answer, size, answer.size());
    }
    
    protected BrokerService createBroker() throws Exception {
        XBeanBrokerFactory factory = new XBeanBrokerFactory();
        BrokerService answer = factory.createBroker(new URI(getBrokerConfigUri()));
        
        // lets disable persistence as we are a test
        answer.setPersistent(false);
        
        return answer;
    }

    protected String getBrokerConfigUri() {
        return "org/apache/activemq/broker/destinations-on-start.xml";
    }
}
