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

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;

public class DestinationMapMemoryTest extends TestCase {


    public void testLongDestinationPath() throws Exception {
        ActiveMQTopic d1 = new ActiveMQTopic("1.2.3.4.5.6.7.8.9.10.11.12.13.14.15.16.17.18");
        DestinationMap map = new DestinationMap();
        map.put(d1, d1);
    }

    public void testVeryLongestinationPaths() throws Exception {

        for (int i = 1; i < 100; i++) {
            String name = "1";
            for (int j = 2; j <= i; j++) {
                name += "." + j;
            }
            //System.out.println("Checking: " + name);
            try {
                ActiveMQDestination d1 = createDestination(name);
                DestinationMap map = new DestinationMap();
                map.put(d1, d1);
            }
            catch (Throwable e) {
                fail("Destination name too long: " + name + " : " + e);
            }
        }
    }

    protected ActiveMQDestination createDestination(String name) {
        return new ActiveMQTopic(name);
    }
}
