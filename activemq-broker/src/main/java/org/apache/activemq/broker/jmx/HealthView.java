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

public class HealthView implements HealthViewMBean {

    ManagedRegionBroker broker;

    public HealthView(ManagedRegionBroker broker) {
        this.broker = broker;
    }

    @Override
    public TabularData health() throws Exception {
        OpenTypeSupport.OpenTypeFactory factory = OpenTypeSupport.getFactory(HealthStatus.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("HealthStatus", "HealthStatus", ct, new String[]{"healthId", "level", "message", "resource"});
        TabularDataSupport rc = new TabularDataSupport(tt);

        List<HealthStatus> list = healthList();
        for (HealthStatus healthStatus : list) {
            rc.put(new CompositeDataSupport(ct, factory.getFields(healthStatus)));
        }
        return rc;
    }

    @Override
    public List<HealthStatus> healthList() throws Exception {
        List<HealthStatus> answer = new ArrayList<HealthStatus>();
        Map<ObjectName, DestinationView> queueViews = broker.getQueueViews();
        for (Map.Entry<ObjectName, DestinationView> entry : queueViews.entrySet()) {
            DestinationView queue = entry.getValue();
            if (queue.getConsumerCount() == 0 && queue.getProducerCount() > 0) {
                ObjectName key = entry.getKey();
                String message = "Queue " + queue.getName() + " has no consumers";
                answer.add(new HealthStatus("org.apache.activemq.noConsumer", "WARNING", message, key.toString()));
            }
        }
        return answer;
    }

}
