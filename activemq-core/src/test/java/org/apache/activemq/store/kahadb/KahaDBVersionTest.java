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
package org.apache.activemq.store.kahadb;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.IOHelper;

import javax.jms.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

/**
 * @author chirino
 */
public class KahaDBVersionTest extends TestCase {

    final static File VERSION_1_DB= new File("src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion1");
    protected BrokerService createBroker(KahaDBPersistenceAdapter kaha) throws Exception {

        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(kaha);
        broker.start();
        return broker;

    }

        
    public void XtestCreateStore() throws Exception {
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        File dir = new File("src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion1");
        IOHelper.deleteFile(dir);
        kaha.setDirectory(dir);
        kaha.setJournalMaxFileLength(1024*1024);
        BrokerService broker = createBroker(kaha);
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.setClientID("test");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.topic");
        Queue queue = session.createQueue("test.queue");
        MessageConsumer consumer = session.createDurableSubscriber(topic,"test");
        consumer.close();
        MessageProducer producer = session.createProducer(topic);
        for (int i =0; i < 1000; i++) {
            Message msg = session.createTextMessage("test message:"+i);
            producer.send(msg);
        }
        producer = session.createProducer(queue);
        for (int i =0; i < 1000; i++) {
            Message msg = session.createTextMessage("test message:"+i);
            producer.send(msg);
        }
        connection.stop();
        broker.stop();
    }
    
    public void testVersionConversion() throws Exception{
        File testDir = new File("target/activemq-data/kahadb/versionDB");
        IOHelper.deleteFile(testDir);
        IOHelper.copyFile(VERSION_1_DB, testDir);
        
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        kaha.setDirectory(testDir);
        kaha.setJournalMaxFileLength(1024*1024);
        BrokerService broker = createBroker(kaha);
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.setClientID("test");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.topic");
        Queue queue = session.createQueue("test.queue");
        MessageConsumer queueConsumer = session.createConsumer(queue);
        for (int i = 0; i < 1000; i++) {
            TextMessage msg  = (TextMessage) queueConsumer.receive(10000);
            //System.err.println(msg.getText());
            assertNotNull(msg);
        }
        MessageConsumer topicConsumer = session.createDurableSubscriber(topic,"test");
        for (int i = 0; i < 1000; i++) {
            TextMessage msg  = (TextMessage) topicConsumer.receive(10000);
            //System.err.println(msg.getText());
            assertNotNull(msg);
        }
        broker.stop();
        
    }

    


    

}