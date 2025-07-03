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

import org.apache.activemq.schema.core.DtoNetworkConnector;

import java.util.List;

public class NetworkConnectorsProcessor extends DefaultConfigurationProcessor {

    public NetworkConnectorsProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public ConfigurationProcessor findProcessor(Object o) {
        if (o instanceof DtoNetworkConnector) {
            return new NetworkConnectorProcessor(plugin, o.getClass());
        }
        return super.findProcessor(o);
    }

    @Override
    protected void applyModifications(List<Object> current, List<Object> modification) {
        outer:
        for (Object modObj : modification) {
            ConfigurationProcessor processor = findProcessor(modObj);
            for (Object currentObj : current) {
                if (modObj.equals(currentObj)) {
                    // If the object is already in the current list, we can skip it
                    plugin.debug("Skipping unchanged network connector: " + modObj);
                    continue outer; // Skip to the next modObj
                } else {
                    // if the modObj doesn't match, remove it
                    if (processor != null) {
                        processor.remove(currentObj);
                    } else {
                        remove(currentObj);
                    }
                }
            }
            // If we reach here, it means the modObj is not in the current list
            if (processor != null) {
                processor.addNew(modObj);
            } else {
                addNew(modObj);
            }
        }
    }
}
