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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import org.apache.activemq.schema.core.DtoQueue;
import org.apache.activemq.schema.core.DtoTopic;

import java.util.Arrays;
import java.util.List;

public class DestinationsProcessor extends DefaultConfigurationProcessor {

    public DestinationsProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public void processChanges(List current, List modified) {
        for (Object destinations : modified) {
            for (Object dto : getContents(destinations)) {
                try {
                    ActiveMQDestination destination = createDestination(dto);
                    if (!containsDestination(destination)) {
                        plugin.addDestination(plugin.getBrokerService().getAdminConnectionContext(), destination, true);
                        plugin.info("Added destination " + destination);
                    }
                } catch (Exception e) {
                    plugin.info("Failed to add a new destination for DTO: " + dto, e);
                }
            }
        }
    }

    protected boolean containsDestination(ActiveMQDestination destination) throws Exception {
        return Arrays.asList(plugin.getBrokerService().getRegionBroker().getDestinations()).contains(destination);
    }

    @Override
    public void addNew(Object o) {
        try {
            ActiveMQDestination destination = createDestination(o);
            plugin.addDestination(plugin.getBrokerService().getAdminConnectionContext(), destination, true);
            plugin.info("Added destination " + destination);
        } catch (Exception e) {
            plugin.info("Failed to add a new destination for DTO: " + o, e);
        }
    }

    private ActiveMQDestination createDestination(Object dto) throws Exception {
        if (dto instanceof DtoQueue) {
            return new ActiveMQQueue(((DtoQueue) dto).getPhysicalName());
        } else if (dto instanceof DtoTopic) {
            return new ActiveMQTopic(((DtoTopic) dto).getPhysicalName());
        } else {
            throw new Exception("Unknown destination type for DTO " + dto);
        }
    }
}
