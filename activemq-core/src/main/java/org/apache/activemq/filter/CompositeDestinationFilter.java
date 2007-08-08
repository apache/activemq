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
package org.apache.activemq.filter;

import org.apache.activemq.command.ActiveMQDestination;

/**
 * A {@link DestinationFilter} used for composite destinations
 *
 * @version $Revision: 1.3 $
 */
public class CompositeDestinationFilter extends DestinationFilter {
    
    private DestinationFilter filters[];

    public CompositeDestinationFilter(ActiveMQDestination destination) {
        ActiveMQDestination[] destinations = destination.getCompositeDestinations();
        filters = new DestinationFilter[destinations.length];
        for (int i = 0; i < destinations.length; i++) {
            ActiveMQDestination childDestination = destinations[i];
            filters[i]= DestinationFilter.parseFilter(childDestination);
        }
    }

    public boolean matches(ActiveMQDestination destination) {
        for (int i = 0; i < filters.length; i++) {
            if (filters[i].matches(destination)) {
                return true;
            }
        }
        return false;
    }

    public boolean isWildcard() {
        return true;
    }
}
