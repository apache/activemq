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
package org.apache.activemq.advisory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.region.RegionBroker;

/**
 * @version $Revision: 397249 $
 */
public class TempDestLoadTest extends EmbeddedBrokerTestSupport {
  
    protected int consumerCounter;
    private Connection connection;
    private Session session;
    private static final int MESSAGE_COUNT = 2000;
        
    public void testLoadTempAdvisoryQueues() throws Exception {

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            TemporaryQueue tempQueue = session.createTemporaryQueue();
            MessageConsumer consumer = session.createConsumer(tempQueue);
            MessageProducer producer = session.createProducer(tempQueue);
            consumer.close();
            producer.close();
            tempQueue.delete();
        }

        AdvisoryBroker ab = (AdvisoryBroker) broker.getBroker().getAdaptor(
                AdvisoryBroker.class);
               
        assertTrue(ab.getAdvisoryDestinations().size() == 0);
        assertTrue(ab.getAdvisoryConsumers().size() == 0);
        assertTrue(ab.getAdvisoryProducers().size() == 0);
               
        RegionBroker rb = (RegionBroker) broker.getBroker().getAdaptor(
                RegionBroker.class);
        //there should be 3 destinations - advisories - 
        //1 for the connection + 2 generic ones
        assertTrue(rb.getDestinationMap().size()==3);            
    }
    
    public void testLoadTempAdvisoryTopics() throws Exception {
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            TemporaryTopic tempTopic =  session.createTemporaryTopic();
            MessageConsumer consumer = session.createConsumer(tempTopic);
            MessageProducer producer = session.createProducer(tempTopic);
            consumer.close();
            producer.close();
            tempTopic.delete();
        }

        AdvisoryBroker ab = (AdvisoryBroker) broker.getBroker().getAdaptor(
                AdvisoryBroker.class);
        assertTrue(ab.getAdvisoryDestinations().size() == 0);
        assertTrue(ab.getAdvisoryConsumers().size() == 0);
        assertTrue(ab.getAdvisoryProducers().size() == 0);
        RegionBroker rb = (RegionBroker) broker.getBroker().getAdaptor(
                RegionBroker.class);
        //there should be 3 destinations - advisories - 
        //1 for the connection + 2 generic ones
        assertTrue(rb.getDestinationMap().size()==3);        
        
    }

    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
        connection.start();
        session = connection.createSession(false, 0);

    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }


}
