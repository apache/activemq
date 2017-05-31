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

import java.util.List;
import org.apache.activemq.schema.core.DtoPolicyMap;
import org.apache.activemq.schema.core.DtoPolicyEntry;

public class PolicyMapProcessor extends DefaultConfigurationProcessor {

    public PolicyMapProcessor(RuntimeConfigurationBroker plugin, Class configurationClass) {
        super(plugin, configurationClass);
    }

    @Override
    public void modify(Object existing, Object candidate) {
        List<Object> existingEntries = filter(existing, DtoPolicyMap.PolicyEntries.class);
        List<Object> candidateEntries = filter(candidate, DtoPolicyMap.PolicyEntries.class);
        // walk the map for mods
        applyModifications(getContents(existingEntries.get(0)), getContents(candidateEntries.get(0)));
    }

    @Override
    public ConfigurationProcessor findProcessor(Object o) {
        if (o instanceof DtoPolicyEntry) {
            return new PolicyEntryProcessor(plugin, o.getClass());
        }
        return super.findProcessor(o);
    }
}
