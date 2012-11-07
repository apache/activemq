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
package org.apache.activemq.broker.region.policy;

import java.util.List;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;

/**
 * Represents a destination based configuration of policies so that individual
 * destinations or wildcard hierarchies of destinations can be configured using
 * different policies.
 * 
 * @org.apache.xbean.XBean
 * 
 * 
 */
public class RedeliveryPolicyMap extends DestinationMap {

    private RedeliveryPolicy defaultEntry;

    public RedeliveryPolicy getEntryFor(ActiveMQDestination destination) {
        RedeliveryPolicy answer = (RedeliveryPolicy) chooseValue(destination);
        if (answer == null) {
            answer = getDefaultEntry();
        }
        return answer;
    }

    /**
     * Sets the individual entries on the redeliveryPolicyMap
     * 
     * @org.apache.xbean.ElementType class="org.apache.activemq.RedeliveryPolicy"
     */
    public void setRedeliveryPolicyEntries(List entries) {
        super.setEntries(entries);
    }

    public RedeliveryPolicy getDefaultEntry() {
        return defaultEntry;
    }

    public void setDefaultEntry(RedeliveryPolicy defaultEntry) {
        this.defaultEntry = defaultEntry;
    }

    protected Class<? extends DestinationMapEntry> getEntryClass() {
        return RedeliveryPolicy.class;
    }
}
