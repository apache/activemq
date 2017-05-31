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
package org.apache.activemq.advisory;

import org.apache.activemq.broker.region.virtual.CompositeDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.filter.DestinationFilter;

/**
 * This class will use a destination filter to see if the activeMQ destination matches
 * the given virtual destination
 *
 */
public class DestinationFilterVirtualDestinationMatcher implements VirtualDestinationMatcher {

    /* (non-Javadoc)
     * @see org.apache.activemq.advisory.VirtualDestinationMatcher#matches(org.apache.activemq.broker.region.virtual.VirtualDestination)
     */
    @Override
    public boolean matches(VirtualDestination virtualDestination, ActiveMQDestination activeMQDest) {
        if (virtualDestination instanceof CompositeDestination) {
            DestinationFilter filter = DestinationFilter.parseFilter(virtualDestination.getMappedDestinations());
            if (filter.matches(activeMQDest)) {
                return true;
            }
        } else if (virtualDestination instanceof VirtualTopic) {
            DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue(((VirtualTopic) virtualDestination).getPrefix() + DestinationFilter.ANY_DESCENDENT));
            if (filter.matches(activeMQDest)) {
                return true;
            }
        }

        return false;
    }

}
