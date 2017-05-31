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

package org.apache.activemq.leveldb.replicated;

import org.apache.activemq.broker.jmx.MBeanInfo;

import javax.management.openmbean.CompositeData;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface ReplicatedLevelDBStoreViewMBean {

    @MBeanInfo("The address of the ZooKeeper server.")
    String getZkAddress();
    @MBeanInfo("The path in ZooKeeper to hold master elections.")
    String getZkPath();
    @MBeanInfo("The ZooKeeper session timeout.")
    String getZkSessionTimeout();
    @MBeanInfo("The address and port the master will bind for the replication protocol.")
    String getBind();
    @MBeanInfo("The number of replication nodes that will be part of the replication cluster.")
    int getReplicas();

    @MBeanInfo("The role of this node in the replication cluster.")
    String getNodeRole();

    @MBeanInfo("The replication status.")
    String getStatus();

    @MBeanInfo("The status of the connected slaves.")
    CompositeData[] getSlaves();

    @MBeanInfo("The current position of the replication log.")
    Long getPosition();

    @MBeanInfo("When the last entry was added to the replication log.")
    Long getPositionDate();

    @MBeanInfo("The directory holding the data.")
    String getDirectory();

    @MBeanInfo("The sync strategy to use.")
    String getSync();

    @MBeanInfo("The node id of this replication node.")
    String getNodeId();
}
