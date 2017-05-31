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

import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.*;
import org.apache.activemq.schema.core.DtoAuthorizationPlugin;
import org.apache.activemq.schema.core.DtoAuthorizationMap;
import org.apache.activemq.schema.core.DtoAuthorizationEntry;

import java.util.LinkedList;
import java.util.List;

public class AuthorizationPluginProcessor extends DefaultConfigurationProcessor {

    public AuthorizationPluginProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public void modify(Object existing, Object candidate) {
        try {
            // replace authorization map - need exclusive write lock to total broker
            AuthorizationBroker authorizationBroker =
                    (AuthorizationBroker) plugin.getBrokerService().getBroker().getAdaptor(AuthorizationBroker.class);

            authorizationBroker.setAuthorizationMap(fromDto(filter(candidate, DtoAuthorizationPlugin.Map.class)));
        } catch (Exception e) {
            plugin.info("failed to apply modified AuthorizationMap to AuthorizationBroker", e);
        }
    }

    private AuthorizationMap fromDto(List<Object> map) {
        XBeanAuthorizationMap xBeanAuthorizationMap = new XBeanAuthorizationMap();
        for (Object o : map) {
            if (o instanceof DtoAuthorizationPlugin.Map) {
                DtoAuthorizationPlugin.Map dtoMap = (DtoAuthorizationPlugin.Map) o;
                List<DestinationMapEntry> entries = new LinkedList<DestinationMapEntry>();
                // revisit - would like to map getAuthorizationMap to generic getContents
                for (Object authMap : filter(dtoMap.getAuthorizationMap(), DtoAuthorizationMap.AuthorizationEntries.class)) {
                    for (Object entry : filter(getContents(authMap), DtoAuthorizationEntry.class)) {
                        entries.add(fromDto(entry, new XBeanAuthorizationEntry()));
                    }
                }
                xBeanAuthorizationMap.setAuthorizationEntries(entries);
                xBeanAuthorizationMap.setGroupClass(dtoMap.getAuthorizationMap().getGroupClass());
                try {
                    xBeanAuthorizationMap.afterPropertiesSet();
                } catch (Exception e) {
                    plugin.info("failed to update xBeanAuthorizationMap auth entries:", e);
                }

                for (Object entry : filter(dtoMap.getAuthorizationMap(), DtoAuthorizationMap.TempDestinationAuthorizationEntry.class)) {
                    // another restriction - would like to be getContents
                    DtoAuthorizationMap.TempDestinationAuthorizationEntry dtoEntry = (DtoAuthorizationMap.TempDestinationAuthorizationEntry) entry;
                    xBeanAuthorizationMap.setTempDestinationAuthorizationEntry(fromDto(dtoEntry.getTempDestinationAuthorizationEntry(), new TempDestinationAuthorizationEntry()));
                }
            } else {
                plugin.info("No support for updates to: " + o);
            }
        }
        return xBeanAuthorizationMap;
    }
}
