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

import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.virtual.*;
import org.apache.activemq.schema.core.DtoVirtualDestinationInterceptor;
import org.apache.activemq.schema.core.DtoVirtualTopic;
import org.apache.activemq.schema.core.DtoCompositeTopic;
import org.apache.activemq.schema.core.DtoCompositeQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VirtualDestinationInterceptorProcessor extends DefaultConfigurationProcessor {

    public VirtualDestinationInterceptorProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public void addNew(Object o) {
        final DtoVirtualDestinationInterceptor dto = (DtoVirtualDestinationInterceptor) o;
        plugin.addDestinationWork.add(new Runnable() {
            public void run() {

                boolean updatedExistingInterceptor = false;
                RegionBroker regionBroker = (RegionBroker) plugin.getBrokerService().getRegionBroker();

                for (DestinationInterceptor destinationInterceptor : plugin.getBrokerService().getDestinationInterceptors()) {
                    if (destinationInterceptor instanceof VirtualDestinationInterceptor) {
                        // update existing interceptor
                        final VirtualDestinationInterceptor virtualDestinationInterceptor =
                                (VirtualDestinationInterceptor) destinationInterceptor;

                        virtualDestinationInterceptor.setVirtualDestinations(fromDto(dto));
                        plugin.info("applied updates to: " + virtualDestinationInterceptor);
                        updatedExistingInterceptor = true;
                    }
                }

                if (!updatedExistingInterceptor) {
                    // add
                    VirtualDestinationInterceptor virtualDestinationInterceptor =
                            new VirtualDestinationInterceptor();
                    virtualDestinationInterceptor.setVirtualDestinations(fromDto(dto));

                    List<DestinationInterceptor> interceptorsList = new ArrayList<DestinationInterceptor>();
                    interceptorsList.addAll(Arrays.asList(plugin.getBrokerService().getDestinationInterceptors()));
                    interceptorsList.add(virtualDestinationInterceptor);

                    DestinationInterceptor[] destinationInterceptors = interceptorsList.toArray(new DestinationInterceptor[]{});
                    plugin.getBrokerService().setDestinationInterceptors(destinationInterceptors);

                    ((CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor()).setInterceptors(destinationInterceptors);
                    plugin.info("applied new: " + interceptorsList);
                }
                regionBroker.reapplyInterceptor();
            }
        });
    }

    @Override
    public void remove(Object o) {
        // whack it
        plugin.addDestinationWork.add(new Runnable() {
            public void run() {
                List<DestinationInterceptor> interceptorsList = new ArrayList<DestinationInterceptor>();
                for (DestinationInterceptor candidate : plugin.getBrokerService().getDestinationInterceptors()) {
                    if (!(candidate instanceof VirtualDestinationInterceptor)) {
                        interceptorsList.add(candidate);
                    }
                }
                DestinationInterceptor[] destinationInterceptors = interceptorsList.toArray(new DestinationInterceptor[]{});
                plugin.getBrokerService().setDestinationInterceptors(destinationInterceptors);
                ((CompositeDestinationInterceptor) ((RegionBroker) plugin.getBrokerService().getRegionBroker()).getDestinationInterceptor()).setInterceptors(destinationInterceptors);
                plugin.info("removed VirtualDestinationInterceptor from: " + interceptorsList);
            }
        });
    }

    private VirtualDestination[] fromDto(DtoVirtualDestinationInterceptor virtualDestinationInterceptor) {
        List<VirtualDestination> answer = new ArrayList<VirtualDestination>();
        for (Object vd : filter(virtualDestinationInterceptor, DtoVirtualDestinationInterceptor.VirtualDestinations.class)) {
            for (Object vt : filter(vd, DtoVirtualTopic.class)) {
                answer.add(fromDto(vt, new VirtualTopic()));
            }
            for (Object vt : filter(vd, DtoCompositeTopic.class)) {
                answer.add(fromDto(vt, new CompositeTopic()));
            }
            for (Object vt : filter(vd, DtoCompositeQueue.class)) {
                answer.add(fromDto(vt, new CompositeQueue()));
            }
        }
        VirtualDestination[] array = new VirtualDestination[answer.size()];
        answer.toArray(array);
        return array;
    }
}
