/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.network.jms;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import javax.jms.*;
import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.QueueRegion;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class QueueBridgeTest extends TestCase implements MessageListener {
    
    protected static final int MESSAGE_COUNT = 10;
    protected AbstractApplicationContext context;
    protected QueueConnection localConnection;
    protected QueueConnection remoteConnection;
    protected QueueRequestor requestor;
    protected QueueSession requestServerSession;
    protected MessageConsumer requestServerConsumer;
    protected MessageProducer requestServerProducer;

    protected void setUp() throws Exception {
        
        super.setUp();
        context = new ClassPathXmlApplicationContext("org/apache/activemq/network/jms/queue-config.xml");
        ActiveMQConnectionFactory fac = (ActiveMQConnectionFactory) context.getBean("localFactory");
        localConnection = fac.createQueueConnection();
        localConnection.start();
        requestServerSession = localConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
        Queue theQueue = requestServerSession.createQueue(getClass().getName());
        requestServerConsumer = requestServerSession.createConsumer(theQueue);
        requestServerConsumer.setMessageListener(this);
        requestServerProducer = requestServerSession.createProducer(null);
        
        fac = (ActiveMQConnectionFactory) context.getBean("remoteFactory");
        remoteConnection = fac.createQueueConnection();
        remoteConnection.start();
        QueueSession session = remoteConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
        requestor = new QueueRequestor(session,theQueue);
    }

    
    protected void tearDown() throws Exception {
        localConnection.close();
        super.tearDown();
    }
    
    public void testQueueRequestorOverBridge() throws JMSException{
        for (int i =0;i < MESSAGE_COUNT; i++){
            TextMessage msg = requestServerSession.createTextMessage("test msg: " +i);
            TextMessage result = (TextMessage) requestor.request(msg);
            assertNotNull(result);
            System.out.println(result.getText());
        }
    }
    
    public void onMessage(Message msg){
        try{
            TextMessage textMsg=(TextMessage) msg;
            String payload="REPLY: "+textMsg.getText();
            Destination replyTo;
            replyTo=msg.getJMSReplyTo();
            textMsg.clearBody();
            textMsg.setText(payload);
            requestServerProducer.send(replyTo,textMsg);
        }catch(JMSException e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
