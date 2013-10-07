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
package org.apache.activemq.broker.partition;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.partition.PartitionBrokerPlugin;
import org.apache.activemq.partition.dto.Partitioning;

/**
 */
public class SpringPartitionBrokerTest extends TestCase {

    public void testCreatePartitionBroker() throws Exception {

        BrokerService broker = BrokerFactory.createBroker("xbean:activemq-partition.xml");
        assertEquals(1, broker.getPlugins().length);
        PartitionBrokerPlugin plugin = (PartitionBrokerPlugin)broker.getPlugins()[0];
        Partitioning config = plugin.getConfig();
        assertEquals(2,  config.getBrokers().size());

        Object o;
        String json = "{\n" +
        "  \"by_client_id\":{\n" +
        "    \"client1\":{\"ids\":[\"broker1\"]},\n" +
        "    \"client2\":{\"ids\":[\"broker1\",\"broker2\"]}\n" +
        "  },\n" +
        "  \"brokers\":{\n" +
        "    \"broker1\":\"tcp://localhost:61616\",\n" +
        "    \"broker2\":\"tcp://localhost:61616\"\n" +
        "  }\n" +
        "}";
        Partitioning expected = Partitioning.MAPPER.readValue(json, Partitioning.class);
        assertEquals(expected.toString(), config.toString());

    }

}
