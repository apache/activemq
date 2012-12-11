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
package org.apache.activemq.broker.jmx;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StatusView implements StatusViewMBean {

    ManagedRegionBroker broker;

    public StatusView(ManagedRegionBroker broker) {
        this.broker = broker;
    }

    @Override
    public TabularData health() throws Exception {
        OpenTypeSupport.OpenTypeFactory factory = OpenTypeSupport.getFactory(StatusEvent.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("Status", "Status", ct, new String[]{"id", "resource"});
        TabularDataSupport rc = new TabularDataSupport(tt);

        List<StatusEvent> list = healthList();
        for (StatusEvent statusEvent : list) {
            rc.put(new CompositeDataSupport(ct, factory.getFields(statusEvent)));
        }
        return rc;
    }

    @Override
    public List<StatusEvent> healthList() throws Exception {
        List<StatusEvent> answer = new ArrayList<StatusEvent>();
        Map<ObjectName, DestinationView> queueViews = broker.getQueueViews();
        for (Map.Entry<ObjectName, DestinationView> entry : queueViews.entrySet()) {
            DestinationView queue = entry.getValue();
            if (queue.getConsumerCount() == 0 && queue.getProducerCount() > 0) {
                answer.add(new StatusEvent("AMQ-NoConsumer", entry.getKey().toString()));
            }
        }
        return answer;
    }

}
