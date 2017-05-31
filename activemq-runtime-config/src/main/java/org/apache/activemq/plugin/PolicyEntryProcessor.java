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

import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.plugin.util.PolicyEntryUtil;

public class PolicyEntryProcessor extends DefaultConfigurationProcessor {

    public PolicyEntryProcessor(RuntimeConfigurationBroker plugin, Class<?> configurationClass) {
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
        PolicyEntry updatedEntry = fromDto(candidate, new PolicyEntry());

        //Look up an existing entry that matches the candidate
        //First just look up by the destination type to see if anything matches
        PolicyEntry existingEntry = PolicyEntryUtil.findEntryByDestination(plugin, updatedEntry);
        if (existingEntry != null) {
            //if found, update the policy and apply the updates to existing destinations
            updatedEntry = fromDto(candidate, existingEntry);
            applyRetrospectively(updatedEntry);
            plugin.info("updated policy for: " + updatedEntry.getDestination());
        } else {
            plugin.info("cannot find policy entry candidate to update: " + updatedEntry + ", destination:" + updatedEntry.getDestination());
        }
    }

    protected void applyRetrospectively(PolicyEntry updatedEntry) {
        PolicyEntryUtil.applyRetrospectively(plugin, updatedEntry);
    }
}
