/*
 * Copyright 2005-2006 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;

import java.util.Iterator;
import java.util.Set;

/**
 * Implements <a href="http://incubator.apache.org/activemq/virtual-destinations.html">Virtual Topics</a>.
 * 
 * @version $Revision: $
 */
public class VirtualTopicBroker extends BrokerPluginSupport {

    public static final String VIRTUAL_WILDCARD = "ActiveMQ.Virtual.*.";

    public VirtualTopicBroker() {
    }
    
    public VirtualTopicBroker(Broker broker) {
        setNext(broker);
    }

    public void send(ConnectionContext ctx, Message message) throws Exception {

        String name = message.getDestination().getPhysicalName();

        String virtualName = VIRTUAL_WILDCARD + name;

        Set destinations = getDestinations(new ActiveMQQueue(virtualName));

        for (Iterator iter = destinations.iterator(); iter.hasNext();) {
            Destination dest = (Destination) iter.next();
            dest.send(ctx, message);
        }
        getNext().send(ctx, message);
    }
}