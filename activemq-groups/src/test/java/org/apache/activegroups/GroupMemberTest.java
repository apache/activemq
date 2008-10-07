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
package org.apache.activegroups;

import java.util.ArrayList;
import java.util.List;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;


public class GroupMemberTest extends TestCase {
    protected BrokerService broker;
    protected String bindAddress = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL;

        
    public void testCoordinatorSelection() throws Exception{
        Group group = new Group(null,"");
        List<Member>list = new ArrayList<Member>();
        final int number =10;
        Member choosen = null;
        for (int i =0;i< number;i++) {
            Member m = new Member("group"+i);
            m.setId(""+i);
            if (number/2==i) {
                m.setCoordinatorWeight(10);
                choosen=m;
            }
            list.add(m);
        }
        Member c = group.selectCordinator(list);
        assertEquals(c,choosen);
    }
    /**
     * Test method for
     * {@link org.apache.activemq.group.Group#addMemberChangedListener(org.apache.activemq.group.MemberChangedListener)}.
     * @throws Exception 
     */
    public void testGroup() throws Exception {
        
        final int number = 10;
        List<Group>groupMaps = new ArrayList<Group>();
        ConnectionFactory factory = createConnectionFactory();
        for (int i =0; i < number; i++) {
            Connection connection = factory.createConnection();
            Group map = new Group(connection,"map"+i);
            map.setHeartBeatInterval(200);
            map.setMinimumGroupSize(i+1);
            map.start();
            groupMaps.add(map);
        }
        
        int coordinatorNumber = 0;
        for (Group map:groupMaps) {
            if (map.isCoordinator()) {
                coordinatorNumber++;
            }
        }
        for(Group map:groupMaps) {
            map.stop();
        }
        
    }
    
public void testWeightedGroup() throws Exception {
        
        final int number = 10;
        List<Group>groupMaps = new ArrayList<Group>();
        Group last = null;
        ConnectionFactory factory = createConnectionFactory();
        for (int i =0; i < number; i++) {
            Connection connection = factory.createConnection();
            Group map = new Group(connection,"map"+i);
            if(i ==number/2) {
                map.setCoordinatorWeight(10);
                last=map;
            }
            
            map.setMinimumGroupSize(i+1);
            map.start();
            groupMaps.add(map);
        }
        Thread.sleep(2000);
        int coordinator = 0;
        Group groupCoordinator = null;
        for (Group map:groupMaps) {
            if (map.isCoordinator()) {
                coordinator++;
                groupCoordinator=map;
            }
        }
             
        
        assertNotNull(groupCoordinator);
        assertEquals(1,coordinator);
        assertEquals(last.getName(),groupCoordinator.getName());
        
        for(Group map:groupMaps) {
            map.stop();
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

