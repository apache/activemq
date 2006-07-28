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
package org.apache.activemq.broker.region.virtual;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.DestinationMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Implements <a
 * href="http://incubator.apache.org/activemq/virtual-destinations.html">Virtual
 * Topics</a>.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class VirtualDestinationInterceptor implements DestinationInterceptor {

    private DestinationMap destinationMap = new DestinationMap();
    private VirtualDestination[] virtualDestinations;

    public Destination intercept(Destination destination) {
        Set virtualDestinations = destinationMap.get(destination.getActiveMQDestination());
        List destinations = new ArrayList();
        for (Iterator iter = virtualDestinations.iterator(); iter.hasNext();) {
            VirtualDestination virtualDestination = (VirtualDestination) iter.next();
            Destination newNestination = virtualDestination.intercept(destination);
            destinations.add(newNestination);
        }
        if (!destinations.isEmpty()) {
            if (destinations.size() == 1) {
                return (Destination) destinations.get(0);
            }
            else {
                // should rarely be used but here just in case
                return createCompositeDestination(destination, destinations);
            }
        }
        return destination;
    }

    public VirtualDestination[] getVirtualDestinations() {
        return virtualDestinations;
    }

    public void setVirtualDestinations(VirtualDestination[] virtualDestinations) {
        destinationMap = new DestinationMap();
        this.virtualDestinations = virtualDestinations;
        for (int i = 0; i < virtualDestinations.length; i++) {
            VirtualDestination virtualDestination = virtualDestinations[i];
            destinationMap.put(virtualDestination.getVirtualDestination(), virtualDestination);
        }
    }

    protected Destination createCompositeDestination(Destination destination, final List destinations) {
        return new DestinationFilter(destination) {
            public void send(ConnectionContext context, Message messageSend) throws Exception {
                for (Iterator iter = destinations.iterator(); iter.hasNext();) {
                    Destination destination = (Destination) iter.next();
                    destination.send(context, messageSend);
                }
            }
        };
    }

}