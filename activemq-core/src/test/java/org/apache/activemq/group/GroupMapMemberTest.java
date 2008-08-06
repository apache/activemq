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
package org.apache.activemq.group;

import java.util.ArrayList;
import java.util.List;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;


public class GroupMapMemberTest extends TestCase {
    protected BrokerService broker;
    protected String bindAddress = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL;

    /**
     * Test method for
     * {@link org.apache.activemq.group.GroupMap#addMemberChangedListener(org.apache.activemq.group.MemberChangedListener)}.
     * @throws Exception 
     */
    public void testGroup() throws Exception {
        
        int number = 20;
        List<Connection>connections = new ArrayList<Connection>();
        List<GroupMap>groupMaps = new ArrayList<GroupMap>();
        ConnectionFactory factory = createConnectionFactory();
        for (int i =0; i < number; i++) {
            Connection connection = factory.createConnection();
            connection.start();
            connections.add(connection);
            GroupMap map = new GroupMap(connection,"map"+i);
            map.setHeartBeatInterval(20000);
            if(i ==number-1) {
                map.setMinimumGroupSize(number);
            }
            map.start();
            groupMaps.add(map);
        }
        
        int coordinator = 0;
        for (GroupMap map:groupMaps) {
            if (map.isCoordinator()) {
                coordinator++;
            }
        }
               
        assertEquals(1,coordinator);
        groupMaps.get(0).put("key", "value");
        Thread.sleep(2000);
        for (GroupMap map:groupMaps) {
            assertTrue(map.get("key").equals("value"));
        }
        for(GroupMap map:groupMaps) {
            map.stop();
        }
        for (Connection connection:connections) {
            connection.stop();
        }
        
    }

    

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory()throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                ActiveMQConnection.DEFAULT_BROKER_URL);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setPersistent(false);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}

