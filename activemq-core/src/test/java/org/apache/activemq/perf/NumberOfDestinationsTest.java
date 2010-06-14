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
package org.apache.activemq.perf;

/**
 * A NumberOfDestinationsTest
 *
 */
import java.io.File;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * @version $Revision: 1.3 $
 */
public class NumberOfDestinationsTest extends TestCase {
    protected static final int MESSAGE_COUNT = 1;
    protected static final int NUMBER_OF_DESTINATIONS = 100000;
    private static final Log LOG = LogFactory.getLog(NumberOfDestinationsTest.class);
    protected BrokerService broker;
    protected String bindAddress = "vm://localhost";
    protected int destinationCount;

    public void testDestinations() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer mp = session.createProducer(null);
        for (int j = 0; j < NUMBER_OF_DESTINATIONS; j++) {
            Destination dest = getDestination(session);
          
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                Message msg = session.createTextMessage("test" + i);
                mp.send(dest, msg);
                
            }
            if (j % 500 == 0) {
                LOG.info("Iterator " + j);
            }
        }
        
        connection.close();
    }

    protected Destination getDestination(Session session) throws JMSException {
        String topicName = getClass().getName() + "." + destinationCount++;
        return session.createTopic(topicName);
    }

    @Override
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(bindAddress);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        File dataFileDir = new File("target/test-amq-data/perfTest/kahadb");

        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        kaha.setDirectory(dataFileDir);
        //answer.setUseJmx(false);

        // The setEnableJournalDiskSyncs(false) setting is a little dangerous right now, as I have not verified 
        // what happens if the index is updated but a journal update is lost.
        // Index is going to be in consistent, but can it be repaired?
        //kaha.setEnableJournalDiskSyncs(false);
        // Using a bigger journal file size makes he take fewer spikes as it is not switching files as often.
        //kaha.setJournalMaxFileLength(1024*100);
        
        // small batch means more frequent and smaller writes
        //kaha.setIndexWriteBatchSize(100);
        // do the index write in a separate thread
        //kaha.setEnableIndexWriteAsync(true);
        
        answer.setPersistenceAdapter(kaha);
        answer.setAdvisorySupport(false);
        answer.setEnableStatistics(false);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}
