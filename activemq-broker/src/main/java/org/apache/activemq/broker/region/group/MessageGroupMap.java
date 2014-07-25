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
package org.apache.activemq.broker.region.group;

import java.util.Map;

import org.apache.activemq.command.ConsumerId;

/**
 * Represents a map of JMSXGroupID values to consumer IDs
 * 
 * 
 */
public interface MessageGroupMap {

    void put(String groupId, ConsumerId consumerId);

    ConsumerId get(String groupId);

    ConsumerId removeGroup(String groupId);

    MessageGroupSet removeConsumer(ConsumerId consumerId);

    void removeAll();

    /**
     * @return  a map of group names and associated consumer Id
     */
    Map<String,String> getGroups();

    String getType();

}
