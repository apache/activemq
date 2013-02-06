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
package org.apache.activemq.filter;

import java.util.Set;

import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.util.IdGenerator;

public class DestinationMapTempDestinationTest extends TestCase {

    public void testtestTempDestinations() throws Exception {
        ConnectionId id = new ConnectionId(new IdGenerator().generateId());
        DestinationMap map = new DestinationMap();
        Object value = new Object();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            ActiveMQTempQueue queue = new ActiveMQTempQueue(id, i);
            map.put(queue, value);
        }
        for (int i = 0; i < count; i++) {
            ActiveMQTempQueue queue = new ActiveMQTempQueue(id, i);
            map.remove(queue, value);
            Set<?> set = map.get(queue);
            assertTrue(set.isEmpty());
        }
    }
}