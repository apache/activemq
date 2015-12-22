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
package org.apache.activemq.plugin.util;

import java.util.Set;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.plugin.AbstractRuntimeConfigurationBroker;


public class PolicyEntryUtil {


    /**
     * Find a matching PolicyEntry by looking up the Set of entries from the map and
     * then comparing the destination to find the exact match.  This lets us be able to
     * find the correct policy entry to update even though there might be multiple that
     * are returned from the get method of the PolicyMap.
     *
     * @param runtimeBroker
     * @param entry
     * @return
     */
    public static PolicyEntry findEntryByDestination(AbstractRuntimeConfigurationBroker runtimeBroker,
            PolicyEntry entry) {

        PolicyMap existingMap = runtimeBroker.getBrokerService().getDestinationPolicy();
        @SuppressWarnings("unchecked")
        Set<PolicyEntry> existingEntries = existingMap.get(entry.getDestination());

        //First just look up by the destination type to see if anything matches
        PolicyEntry existingEntry = null;
        for (PolicyEntry ee: existingEntries) {
            if (ee.getDestination().equals(entry.getDestination())) {
                existingEntry = ee;
                break;
            }
        }
        return existingEntry;
    }

    /**
     * Utility to properly apply an updated policy entry to all existing destinations that
     * match this entry.  The destination will only be updated if the policy is the exact
     * policy (most specific) that matches the destination.
     *
     * @param runtimeBroker
     * @param updatedEntry
     */
    public static void applyRetrospectively(AbstractRuntimeConfigurationBroker runtimeBroker,
            PolicyEntry updatedEntry) {
        PolicyEntryUtil.applyRetrospectively(runtimeBroker, updatedEntry, null);
    }

    /**
     *
     * Utility to properly apply an updated policy entry to all existing destinations that
     * match this entry.  The destination will only be updated if the policy is the exact
     * policy (most specific) that matches the destination.
     *
     * The includedProperties List is optional and is used to specify a list of properties
     * to apply retrospectively to the matching destinations. This allows only certain properties
     * to be reapplied.  If the list is null then all properties will be applied.
     *
     * @param runtimeBroker
     * @param updatedEntry
     * @param includedProperties
     */
    public static void applyRetrospectively(AbstractRuntimeConfigurationBroker runtimeBroker,
            PolicyEntry updatedEntry, Set<String> includedProperties) {
        RegionBroker regionBroker = (RegionBroker) runtimeBroker.getBrokerService().getRegionBroker();
        for (Destination destination : regionBroker.getDestinations(updatedEntry.getDestination())) {
            //Look up the policy that applies to the destination
            PolicyEntry specificyPolicy = regionBroker.getDestinationPolicy().getEntryFor(
                    destination.getActiveMQDestination());

            //only update the destination if it matches the specific policy being updated
            //currently just an identity check which is what we want
            if (updatedEntry.equals(specificyPolicy)){
                Destination target = destination;
                while (target instanceof DestinationFilter) {
                    target = ((DestinationFilter)target).getNext();
                }
                //If we are providing a list of properties to set then use them
                //to set eligible properties that are in the includedProperties list
                if (target.getActiveMQDestination().isQueue()) {
                    updatedEntry.update((Queue) target, includedProperties);
                } else if (target.getActiveMQDestination().isTopic()) {
                    updatedEntry.update((Topic) target, includedProperties);
                }
                runtimeBroker.debug("applied update to:" + target);
            }
        }
    }
}
