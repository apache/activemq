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

import org.apache.activemq.schema.core.DtoBroker;

public class ProcessorFactory {

    public static ConfigurationProcessor createProcessor(RuntimeConfigurationBroker plugin, Class dtoClass) {
        if (dtoClass.equals(DtoBroker.Plugins.class)) {
            return new PluginsProcessor(plugin, dtoClass);
        } else if (dtoClass.equals(DtoBroker.NetworkConnectors.class)) {
            return new NetworkConnectorsProcessor(plugin, dtoClass);
        } else if (dtoClass.equals(DtoBroker.DestinationPolicy.class)) {
            return new DestinationPolicyProcessor(plugin, dtoClass);
        } else if (dtoClass.equals(DtoBroker.DestinationInterceptors.class)) {
            return new DestinationInterceptorProcessor(plugin, dtoClass);
        } else if (dtoClass.equals(DtoBroker.Destinations.class)) {
            return new DestinationsProcessor(plugin, dtoClass);
        } else {
            return new DefaultConfigurationProcessor(plugin, dtoClass);
        }
    }

}
