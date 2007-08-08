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
package org.apache.activemq.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Test;

public class ActiveMQDestinationTest extends DataStructureTestSupport {

    public ActiveMQDestination destination;

    public void initCombosForTestDesintaionMarshaling() {
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"),
                                                          new ActiveMQTopic("TEST"),
                                                          new ActiveMQTempQueue("TEST:1"),
                                                          new ActiveMQTempTopic("TEST:1"),
                                                          new ActiveMQTempQueue("TEST"),
                                                          new ActiveMQTempTopic("TEST"),
                                                          new ActiveMQQueue("TEST?option=value"),
                                                          new ActiveMQTopic("TEST?option=value"),
                                                          new ActiveMQTempQueue("TEST:1?option=value"),
                                                          new ActiveMQTempTopic("TEST:1?option=value")});
    }

    public void testDesintaionMarshaling() throws IOException {
        assertBeanMarshalls(destination);
    }

    public void initCombosForTestDesintaionOptions() {
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST?k1=v1&k2=v2"),
                                                          new ActiveMQTopic("TEST?k1=v1&k2=v2"),
                                                          new ActiveMQTempQueue("TEST:1?k1=v1&k2=v2"),
                                                          new ActiveMQTempTopic("TEST:1?k1=v1&k2=v2"),});
    }

    public void testDesintaionOptions() throws IOException {
        Map options = destination.getOptions();
        assertNotNull(options);
        assertEquals("v1", options.get("k1"));
        assertEquals("v2", options.get("k2"));
    }

    public void testSorting() throws Exception {
        SortedSet set = new TreeSet();
        ActiveMQDestination[] destinations = {new ActiveMQQueue("A"), new ActiveMQQueue("B"),
                                              new ActiveMQTopic("A"), new ActiveMQTopic("B")};
        List expected = Arrays.asList(destinations);
        set.addAll(expected);
        List actual = new ArrayList(set);
        assertEquals("Sorted order", expected, actual);
    }

    public static Test suite() {
        return suite(ActiveMQDestinationTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
