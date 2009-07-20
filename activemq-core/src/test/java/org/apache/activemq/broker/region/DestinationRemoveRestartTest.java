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
package org.apache.activemq.broker.region;

import junit.framework.Test;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;

// from https://issues.apache.org/activemq/browse/AMQ-2216
public class DestinationRemoveRestartTest extends CombinationTestSupport {
    private final static String destinationName = "TEST";
    public byte destinationType;
    BrokerService broker;

    protected void setUp() throws Exception {
        broker = createBroker();
    }

    private BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(true);
        broker.start();
        return broker;
    }

    protected void tearDown() throws Exception {
        broker.stop();
    }

    public void initCombosForTestCheckDestinationRemoveActionAfterRestart() {
        addCombinationValues("destinationType", new Object[]{Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }
    
    public void testCheckDestinationRemoveActionAfterRestart() throws Exception {
        doAddDestination();
        doRemoveDestination();
        broker.stop();
        broker.waitUntilStopped();
        broker = createBroker();
        doCheckRemoveActionAfterRestart();
    }

    public void doAddDestination() throws Exception {
        boolean res = false;
        
        ActiveMQDestination amqDestination = 
            ActiveMQDestination.createDestination(destinationName, destinationType);
        broker.getRegionBroker().addDestination(broker.getAdminConnectionContext(), (ActiveMQDestination) amqDestination);
        
        final ActiveMQDestination[] list = broker.getRegionBroker().getDestinations();
        for (final ActiveMQDestination element : list) {
            final Destination destination = broker.getDestination(element);
            if (destination.getActiveMQDestination().getPhysicalName().equals(destinationName)) {                  
                res = true;
                break;
            }
        }
        
        assertTrue("Adding destination Failed", res);        
    }
    
    public void doRemoveDestination() throws Exception {
        boolean res = true;
        
        broker.removeDestination(ActiveMQDestination.createDestination(destinationName, destinationType));
        final ActiveMQDestination[] list = broker.getRegionBroker().getDestinations();
        for (final ActiveMQDestination element : list) {
            final Destination destination = broker.getDestination(element);
            if (destination.getActiveMQDestination().getPhysicalName().equals(destinationName)) {                  
                res = false;
                break;
            }
        }
        
        assertTrue("Removing destination Failed", res);      
    }
    
    
    public void doCheckRemoveActionAfterRestart() throws Exception {
        boolean res = true;
        
        final ActiveMQDestination[] list = broker.getRegionBroker().getDestinations();
        for (final ActiveMQDestination element : list) {
            final Destination destination = broker.getDestination(element);
            if (destination.getActiveMQDestination().getPhysicalName().equals(destinationName)) {                  
                res = false;
                break;
            }
        }
        
        assertTrue("The removed destination is reloaded after restart !", res);
    }
    
    public static Test suite() {
        return suite(DestinationRemoveRestartTest.class);
    }
}
