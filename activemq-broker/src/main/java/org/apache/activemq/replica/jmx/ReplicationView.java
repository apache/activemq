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
package org.apache.activemq.replica.jmx;

import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.ReplicaStatistics;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationView implements ReplicationViewMBean {

    private final ReplicaPlugin plugin;
    private final ReplicaStatistics replicaStatistics;

    public ReplicationView(ReplicaPlugin plugin, ReplicaStatistics replicaStatistics) {
        this.plugin = plugin;
        this.replicaStatistics = replicaStatistics;
    }

    @Override
    public Long getReplicationTps() {
        return Optional.ofNullable(replicaStatistics.getReplicationTps()).map(AtomicLong::get).orElse(null);
    }

    @Override
    public void setReplicationRole(String role, boolean force) throws Exception {
        plugin.setReplicaRole(ReplicaRole.valueOf(role), force);
    }

    @Override
    public String getReplicationRole() {
        return plugin.getRole().name();
    }

    @Override
    public Long getTotalReplicationLag() {
        return Optional.ofNullable(replicaStatistics.getTotalReplicationLag()).map(AtomicLong::get).orElse(null);
    }

    @Override
    public Long getSourceWaitTime() {
        return Optional.ofNullable(replicaStatistics.getSourceLastProcessedTime()).map(AtomicLong::get)
                .map(v -> System.currentTimeMillis() - v).orElse(null);
    }

    @Override
    public Long getReplicationLag() {
        return Optional.ofNullable(replicaStatistics.getReplicationLag()).map(AtomicLong::get).orElse(null);
    }

    @Override
    public Long getReplicaWaitTime() {
        return Optional.ofNullable(replicaStatistics.getReplicaLastProcessedTime()).map(AtomicLong::get)
                .map(v -> System.currentTimeMillis() - v).orElse(null);
    }
}
