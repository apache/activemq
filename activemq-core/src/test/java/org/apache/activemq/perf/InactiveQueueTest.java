/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.perf;

import java.io.File;
import java.net.URI;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.journal.JournalPersistenceAdapterFactory;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 454471 $
 */
public class InactiveQueueTest extends TestCase{
    private static final transient Log log = LogFactory.getLog(InactiveQueueTest.class);

    private static final int MESSAGE_COUNT = 0;
    private static final String DEFAULT_PASSWORD="";
    private static final String USERNAME="testuser";
    private static final String CLIENTID="mytestclient";
    private static final String QUEUE_NAME="testevent";
    private static final int deliveryMode=javax.jms.DeliveryMode.PERSISTENT;
    private static final int deliveryPriority=javax.jms.Message.DEFAULT_PRIORITY;
    private Connection connection=null;
    private MessageProducer publisher=null;
    private TopicSubscriber subscriber=null;
    private Destination destination=null;
    private Session session=null;
    ActiveMQConnectionFactory connectionFactory=null;
    BrokerService broker;

    protected void setUp() throws Exception{
        super.setUp();
        broker=new BrokerService();
        
        //broker.setPersistenceAdapter(new KahaPersistenceAdapter(new File ("TEST_STUFD")));
        /*
        JournalPersistenceAdapterFactory factory = new JournalPersistenceAdapterFactory();
        factory.setDataDirectoryFile(broker.getDataDirectory());
        factory.setTaskRunnerFactory(broker.getTaskRunnerFactory());
        factory.setUseJournal(false);
        broker.setPersistenceFactory(factory);
        */
        broker.addConnector(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
        broker.start();
        connectionFactory=new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
        /*
         * Doesn't matter if you enable or disable these, so just leaving them out for this test case
         * connectionFactory.setAlwaysSessionAsync(true); connectionFactory.setAsyncDispatch(true);
         */
        connectionFactory.setUseAsyncSend(true);
    }

    protected void tearDown() throws Exception{
        super.tearDown();
        broker.stop();
    }

    public void testNoSubscribers() throws Exception{
        connection=connectionFactory.createConnection(USERNAME,DEFAULT_PASSWORD);
        assertNotNull(connection);
        connection.start();
        session=connection.createSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        destination=session.createQueue(QUEUE_NAME);
        assertNotNull(destination);
        publisher=session.createProducer(destination);
        assertNotNull(publisher);
        MapMessage msg=session.createMapMessage();
        assertNotNull(msg);
        msg.setString("key1","value1");
        int loop;
        for(loop=0;loop<MESSAGE_COUNT;loop++){
            msg.setInt("key2",loop);
            publisher.send(msg,deliveryMode,deliveryPriority,Message.DEFAULT_TIME_TO_LIVE);
            if (loop%500==0){
                log.debug("Sent " + loop + " messages");
            }
        }
        Thread.sleep(1000000);
        this.assertEquals(loop,MESSAGE_COUNT);
        publisher.close();
        session.close();
        connection.stop();
        connection.stop();
    }

    
}
