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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.usage.SystemUsage;

public class HealthView implements HealthViewMBean {

    private ManagedRegionBroker broker;
    private volatile String currentState = "Good";

    public HealthView(ManagedRegionBroker broker) {
        this.broker = broker;
    }

    @Override
    public TabularData health() throws Exception {
        OpenTypeSupport.OpenTypeFactory factory = OpenTypeSupport.getFactory(HealthStatus.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("HealthStatus", "HealthStatus", ct, new String[] { "healthId", "level", "message", "resource" });
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

        /**
         * Check persistence store directory limits
         */
        BrokerService brokerService = broker.getBrokerService();
        if (brokerService != null && brokerService.getPersistenceAdapter() != null) {
            PersistenceAdapter adapter = brokerService.getPersistenceAdapter();
            File dir = adapter.getDirectory();
            if (brokerService.isPersistent()) {
                SystemUsage usage = brokerService.getSystemUsage();
                if (dir != null && usage != null) {
                    String dirPath = dir.getAbsolutePath();
                    if (!dir.isAbsolute()) {
                        dir = new File(dirPath);
                    }

                    while (dir != null && !dir.isDirectory()) {
                        dir = dir.getParentFile();
                    }

                    long storeSize = adapter.size();
                    long storeLimit = usage.getStoreUsage().getLimit();
                    long dirFreeSpace = dir.getUsableSpace();

                    if (storeSize != 0 && storeLimit != 0) {
                        int val = (int) ((storeSize * 100) / storeLimit);
                        if (val > 90) {
                            answer.add(new HealthStatus("org.apache.activemq.StoreLimit", "WARNING", "Message Store size is within " + val + "% of its limit",
                                adapter.toString()));
                        }
                    }

                    if ((storeLimit - storeSize) > dirFreeSpace) {
                        String message = "Store limit is " + storeLimit / (1024 * 1024) + " mb, whilst the data directory: " + dir.getAbsolutePath()
                            + " only has " + dirFreeSpace / (1024 * 1024) + " mb of usable space";
                        answer.add(new HealthStatus("org.apache.activemq.FreeDiskSpaceLeft", "WARNING", message, adapter.toString()));
                    }
                }

                File tmpDir = brokerService.getTmpDataDirectory();
                if (tmpDir != null) {

                    String tmpDirPath = tmpDir.getAbsolutePath();
                    if (!tmpDir.isAbsolute()) {
                        tmpDir = new File(tmpDirPath);
                    }

                    long storeSize = usage.getTempUsage().getUsage();
                    long storeLimit = usage.getTempUsage().getLimit();
                    while (tmpDir != null && !tmpDir.isDirectory()) {
                        tmpDir = tmpDir.getParentFile();
                    }

                    if (storeLimit != 0) {
                        int val = (int) ((storeSize * 100) / storeLimit);
                        if (val > 90) {
                            answer.add(new HealthStatus("org.apache.activemq.TempStoreLimit", "WARNING", "TempMessage Store size is within " + val
                                + "% of its limit", adapter.toString()));
                        }
                    }
                }
            }
        }

        if (brokerService != null && brokerService.getJobSchedulerStore() != null) {
            JobSchedulerStore scheduler = brokerService.getJobSchedulerStore();
            File dir = scheduler.getDirectory();
            if (brokerService.isPersistent()) {
                SystemUsage usage = brokerService.getSystemUsage();
                if (dir != null && usage != null) {
                    String dirPath = dir.getAbsolutePath();
                    if (!dir.isAbsolute()) {
                        dir = new File(dirPath);
                    }

                    while (dir != null && !dir.isDirectory()) {
                        dir = dir.getParentFile();
                    }
                    long storeSize = scheduler.size();
                    long storeLimit = usage.getJobSchedulerUsage().getLimit();
                    long dirFreeSpace = dir.getUsableSpace();

                    if (storeSize != 0 && storeLimit != 0) {
                        int val = (int) ((storeSize * 100) / storeLimit);
                        if (val > 90) {
                            answer.add(new HealthStatus("org.apache.activemq.JobSchedulerLimit", "WARNING", "JobSchedulerMessage Store size is within " + val
                                + "% of its limit", scheduler.toString()));
                        }
                    }

                    if ((storeLimit - storeSize) > dirFreeSpace) {
                        String message = "JobSchedulerStore limit is " + storeLimit / (1024 * 1024) + " mb, whilst the data directory: "
                            + dir.getAbsolutePath() + " only has " + dirFreeSpace / (1024 * 1024) + " mb of usable space";
                        answer.add(new HealthStatus("org.apache.activemq.FreeDiskSpaceLeft", "WARNING", message, scheduler.toString()));
                    }
                }
            }
        }

        StringBuilder currentState = new StringBuilder();
        if (answer != null && !answer.isEmpty()) {
            currentState.append("Getting Worried {");
            for (HealthStatus hs : answer) {
                currentState.append(hs).append(" , ");
            }
            currentState.append(" }");
        } else {
            currentState.append("Good");
        }

        this.currentState = currentState.toString();

        return answer;
    }

    @Override
    public String healthStatus() throws Exception {
        // Must invoke healthList in order to update state.
        healthList();

        return getCurrentStatus();
    }

    /**
     * @return String representation of the current Broker state
     */
    @Override
    public String getCurrentStatus() {
        return this.currentState;
    }
}
