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
package org.apache.activemq.plugin;

import org.apache.activemq.broker.region.*;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

import java.util.Set;

public class PolicyEntryProcessor extends DefaultConfigurationProcessor {

    public PolicyEntryProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public void addNew(Object o) {
        PolicyEntry addition = fromDto(o, new PolicyEntry());
        PolicyMap existingMap = plugin.getBrokerService().getDestinationPolicy();
        existingMap.put(addition.getDestination(), addition);
        applyRetrospectively(addition);
        plugin.info("added policy for: " + addition.getDestination());
    }

    @Override
    public void modify(Object existing, Object candidate) {
        PolicyMap existingMap = plugin.getBrokerService().getDestinationPolicy();

        PolicyEntry updatedEntry = fromDto(candidate, new PolicyEntry());

        Set existingEntry = existingMap.get(updatedEntry.getDestination());
        if (existingEntry.size() == 1) {
            updatedEntry = fromDto(candidate, (PolicyEntry) existingEntry.iterator().next());
            applyRetrospectively(updatedEntry);
            plugin.info("updated policy for: " + updatedEntry.getDestination());
        } else {
            plugin.info("cannot modify policy matching multiple destinations: " + existingEntry + ", destination:" + updatedEntry.getDestination());
        }
    }

    protected void applyRetrospectively(PolicyEntry updatedEntry) {
        RegionBroker regionBroker = (RegionBroker) plugin.getBrokerService().getRegionBroker();
        for (Destination destination : regionBroker.getDestinations(updatedEntry.getDestination())) {
            Destination target = destination;
            if (destination instanceof DestinationFilter) {
                target = ((DestinationFilter)destination).getNext();
            }
            if (target.getActiveMQDestination().isQueue()) {
                updatedEntry.update((Queue) target);
            } else if (target.getActiveMQDestination().isTopic()) {
                updatedEntry.update((Topic) target);
            }
            plugin.debug("applied update to:" + target);
        }
    }
}
