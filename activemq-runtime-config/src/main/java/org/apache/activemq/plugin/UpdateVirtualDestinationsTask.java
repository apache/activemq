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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;

public abstract class UpdateVirtualDestinationsTask implements Runnable {

    private final AbstractRuntimeConfigurationBroker plugin;

    public UpdateVirtualDestinationsTask(
            AbstractRuntimeConfigurationBroker plugin) {
        super();
        this.plugin = plugin;
    }

    @Override
    public void run() {

        boolean updatedExistingInterceptor = false;
        RegionBroker regionBroker = (RegionBroker) plugin.getBrokerService()
                .getRegionBroker();

        for (DestinationInterceptor destinationInterceptor : plugin
                .getBrokerService().getDestinationInterceptors()) {
            if (destinationInterceptor instanceof VirtualDestinationInterceptor) {
                // update existing interceptor
                final VirtualDestinationInterceptor virtualDestinationInterceptor = (VirtualDestinationInterceptor) destinationInterceptor;

                virtualDestinationInterceptor
                        .setVirtualDestinations(getVirtualDestinations());
                plugin.info("applied updates to: "
                        + virtualDestinationInterceptor);
                updatedExistingInterceptor = true;
            }
        }

        if (!updatedExistingInterceptor) {
            // add
            VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
            virtualDestinationInterceptor.setVirtualDestinations(getVirtualDestinations());

            List<DestinationInterceptor> interceptorsList = new ArrayList<DestinationInterceptor>();
            interceptorsList.addAll(Arrays.asList(plugin.getBrokerService()
                    .getDestinationInterceptors()));
            interceptorsList.add(virtualDestinationInterceptor);

            DestinationInterceptor[] destinationInterceptors = interceptorsList
                    .toArray(new DestinationInterceptor[] {});
            plugin.getBrokerService().setDestinationInterceptors(
                    destinationInterceptors);

            ((CompositeDestinationInterceptor) regionBroker
                    .getDestinationInterceptor())
                    .setInterceptors(destinationInterceptors);
            plugin.info("applied new: " + interceptorsList);
        }
        regionBroker.reapplyInterceptor();
    }

    protected abstract VirtualDestination[] getVirtualDestinations();
}