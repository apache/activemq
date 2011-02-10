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

package org.apache.activemq.bugs;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2439Test extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ2439Test.class);
    Destination dest;

    
    public void testDuplicatesThroughNetwork() throws Exception {
        assertEquals("received expected amount", 500, receiveExactMessages("BrokerB", 500));
        assertEquals("received expected amount", 500, receiveExactMessages("BrokerB", 500));
        validateQueueStats();
    }
    
    private void validateQueueStats() throws Exception {
       final BrokerView brokerView = brokers.get("BrokerA").broker.getAdminView();
       assertEquals("enequeue is correct", 1000, brokerView.getTotalEnqueueCount());
       
       assertTrue("dequeue is correct", Wait.waitFor(new Wait.Condition() {
           public boolean isSatisified() throws Exception {
               LOG.info("dequeue count (want 1000), is : " + brokerView.getTotalDequeueCount());
               return 1000 == brokerView.getTotalDequeueCount();
           }
       }));
    }

    protected int receiveExactMessages(String brokerName, int msgCount) throws Exception {
        
        BrokerItem brokerItem = brokers.get(brokerName);
        Connection connection = brokerItem.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);       
        MessageConsumer consumer = session.createConsumer(dest);
        
        Message msg;
        int i;
        for (i = 0; i < msgCount; i++) {
            msg = consumer.receive(1000);
            if (msg == null) {
                break;
            }
        }

        connection.close();
        brokerItem.connections.remove(connection);
        
        return i;
    }
    
    public void setUp() throws Exception {
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=true&deleteAllMessagesOnStartup=true&advisorySupport=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=true&deleteAllMessagesOnStartup=true&useJmx=false"));
        bridgeBrokers("BrokerA", "BrokerB");
        
        startAllBrokers();
        
        // Create queue
        dest = createDestination("TEST.FOO", false);
        sendMessages("BrokerA", dest, 1000);
    }   
}
