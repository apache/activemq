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
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;

/**
 * @version $Revision: 397249 $
 */
public class TempQueueMemoryTest extends EmbeddedBrokerTestSupport {
    private Connection serverConnection;
    private Session serverSession;
    private Connection clientConnection;
    private Session clientSession;
    private Destination serverDestination;
    private static final int COUNT = 1000;

    public void testLoadRequestReply() throws Exception {
        MessageConsumer serverConsumer = serverSession.createConsumer(serverDestination);
        serverConsumer.setMessageListener(new MessageListener() {
            public void onMessage(Message msg) {
                try {
                    Destination replyTo = msg.getJMSReplyTo();
                    MessageProducer producer = serverSession.createProducer(replyTo);
                    producer.send(replyTo, msg);
                    producer.close();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
        
        MessageProducer producer = clientSession.createProducer(serverDestination);
        for (int i =0; i< COUNT; i++) {
            TemporaryQueue replyTo = clientSession.createTemporaryQueue();
            MessageConsumer consumer = clientSession.createConsumer(replyTo);
            Message msg = clientSession.createMessage();
            msg.setJMSReplyTo(replyTo);
            producer.send(msg);
            Message reply = consumer.receive();
            consumer.close();
            replyTo.delete();
        }
        
        clientSession.close();
        serverSession.close();
        clientConnection.close();
        serverConnection.close();
        
        AdvisoryBroker ab = (AdvisoryBroker) broker.getBroker().getAdaptor(
                AdvisoryBroker.class);
              
        ///The server destination will be left
        assertTrue(ab.getAdvisoryDestinations().size() == 1);
        
        assertTrue("should be zero but is "+ab.getAdvisoryConsumers().size(),ab.getAdvisoryConsumers().size() == 0);
        assertTrue("should be zero but is "+ab.getAdvisoryProducers().size(),ab.getAdvisoryProducers().size() == 0);
               
        RegionBroker rb = (RegionBroker) broker.getBroker().getAdaptor(
                RegionBroker.class);
        
               
        //serverDestination + 
        assertTrue(rb.getDestinationMap().size()==7);          
    }

    protected void setUp() throws Exception {
        super.setUp();
        serverConnection = createConnection();
        serverConnection.start();
        serverSession = serverConnection.createSession(false, 0);
        clientConnection = createConnection();
        clientConnection.start();
        clientSession = clientConnection.createSession(false, 0);
        serverDestination = createDestination();
    }

    protected void tearDown() throws Exception {
        
        super.tearDown();
    }
    
    protected Destination createDestination() {
        return new ActiveMQQueue(getClass().getName());
    }
}
