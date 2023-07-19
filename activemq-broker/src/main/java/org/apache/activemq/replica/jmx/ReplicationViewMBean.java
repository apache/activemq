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

import org.apache.activemq.broker.jmx.MBeanInfo;

public interface ReplicationViewMBean {

    @MBeanInfo("Replication TPS")
    Long getReplicationTps();

    @MBeanInfo("Set replication role for broker")
    void setReplicationRole(String role, boolean force) throws Exception;

    @MBeanInfo("Get current replication role for broker")
    String getReplicationRole();

    @MBeanInfo("Total replication lag")
    Long getTotalReplicationLag();

    @MBeanInfo("Get wait time(if the broker's role is source)")
    Long getSourceWaitTime();

    @MBeanInfo("Get replication lag")
    Long getReplicationLag();

    @MBeanInfo("Get wait time(if the broker's role is replica)")
    Long getReplicaWaitTime();

    @MBeanInfo("Flow control is enabled for replication on the source side")
    Boolean getSourceReplicationFlowControl();

    @MBeanInfo("Flow control is enabled for replication on the replica side")
    Boolean getReplicaReplicationFlowControl();
}
