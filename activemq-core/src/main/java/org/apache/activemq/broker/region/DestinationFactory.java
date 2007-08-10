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

import java.io.IOException;
import java.util.Set;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;

/**
 * Used to create Destinations. One instance of DestinationFactory is used per BrokerService. 
 * 
 * @author fateev@amazon.com
 * @version $Revision$
 */
public abstract class DestinationFactory {
    
    /**
     * Create destination implementation.
     */
    public abstract Destination createDestination(ConnectionContext context, ActiveMQDestination destination, DestinationStatistics destinationStatistics) throws Exception;

    /**
     * Returns a set of all the {@link org.apache.activemq.command.ActiveMQDestination}
     * objects that the persistence store is aware exist.
     */
    public abstract Set<ActiveMQDestination> getDestinations();

    /**
     * Lists all the durable subscirptions for a given destination.
     */
    public abstract SubscriptionInfo[] getAllDurableSubscriptions(ActiveMQTopic topic) throws IOException;

    
    public abstract long getLastMessageBrokerSequenceId() throws IOException;

    public abstract void setRegionBroker(RegionBroker regionBroker);
}
