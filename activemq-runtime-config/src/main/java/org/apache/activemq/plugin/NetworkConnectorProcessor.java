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

import java.util.Properties;
import java.util.TreeMap;

import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.schema.core.DtoNetworkConnector;
import org.apache.activemq.util.IntrospectionSupport;

public class NetworkConnectorProcessor extends DefaultConfigurationProcessor {

    public NetworkConnectorProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public void addNew(Object o) {
        DtoNetworkConnector networkConnector = (DtoNetworkConnector) o;
        if (networkConnector.getUri() != null) {
            try {
                DiscoveryNetworkConnector nc = fromDto(networkConnector, new DiscoveryNetworkConnector());
                plugin.getBrokerService().addNetworkConnector(nc);
                plugin.getBrokerService().startNetworkConnector(nc, null);
                plugin.info("started new network connector: " + nc);
            } catch (Exception e) {
                plugin.info("Failed to add new networkConnector " + networkConnector, e);
            }
        }
    }

    @Override
    public void remove(Object o) {
        DtoNetworkConnector toRemove = (DtoNetworkConnector) o;
        for (NetworkConnector existingCandidate :
                plugin.getBrokerService().getNetworkConnectors()) {
            if (configMatch(toRemove, existingCandidate)) {
                if (plugin.getBrokerService().removeNetworkConnector(existingCandidate)) {
                    try {
                        existingCandidate.stop();
                        plugin.info("stopped and removed networkConnector: " + existingCandidate);
                    } catch (Exception e) {
                        plugin.info("Failed to stop removed network connector: " + existingCandidate);
                    }
                }
            }
        }
    }

    private boolean configMatch(DtoNetworkConnector dto, NetworkConnector candidate) {
        Properties dtoProps = new Properties();
        IntrospectionSupport.getProperties(dto, dtoProps, null);
        // the live candidate has its ${foo} placeholders already resolved, so resolve them on
        // the dto side too before comparing — otherwise a connector declared with placeholders
        // can never be matched for removal/modification
        if (plugin.placeHolderUtil != null) {
            plugin.placeHolderUtil.filter(dtoProps);
        }

        TreeMap<String, String> candidateProps = new TreeMap<String, String>();
        IntrospectionSupport.getProperties(candidate, candidateProps, null);

        // every dto prop must be present in the candidate
        for (Object keyObj : dtoProps.keySet()) {
            String key = (String) keyObj;
            String dtoValue = (String) dtoProps.get(key);
            if (!candidateProps.containsKey(key) || !candidateProps.get(key).equals(dtoValue)) {
                return false;
            }
        }
        return true;
    }
}
