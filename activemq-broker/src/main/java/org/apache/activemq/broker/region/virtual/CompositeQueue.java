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
import org.apache.activemq.command.ActiveMQQueue;

/**
 * Represents a virtual queue which forwards to a number of other destinations.
 *
 * @org.apache.xbean.XBean
 *
 */
public class CompositeQueue extends CompositeDestination {

    @Override
    public ActiveMQDestination getVirtualDestination() {
        return new ActiveMQQueue(getName());
    }

    @Override
    public Destination interceptMappedDestination(Destination destination) {
        // nothing to do for mapped destinations
        return destination;
    }

    @Override
    public String toString() {
        return "CompositeQueue [" + getName() + "]";
    }
}
