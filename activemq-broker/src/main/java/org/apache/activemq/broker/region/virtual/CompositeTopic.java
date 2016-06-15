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
package org.apache.activemq.broker.region.virtual;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * Represents a virtual topic which forwards to a number of other destinations.
 *
 * @org.apache.xbean.XBean
 *
 */
public class CompositeTopic extends CompositeDestination {

    @Override
    public ActiveMQDestination getVirtualDestination() {
        return new ActiveMQTopic(getName());
    }

    @Override
    public Destination interceptMappedDestination(Destination destination) {
        if (!isForwardOnly() && destination.getActiveMQDestination().isQueue()) {
            // recover retroactive messages in mapped Queue
            return new MappedQueueFilter(getVirtualDestination(), destination);
        }
        return destination;
    }

    @Override
    public String toString() {
        return "CompositeTopic [" + getName() + "]";
    }
}
