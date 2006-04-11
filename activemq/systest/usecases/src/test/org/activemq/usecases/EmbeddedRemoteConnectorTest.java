/**
 *
 * Copyright 2004 Protique Ltd
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
 *
 **/
package org.activemq.usecases;

import org.activemq.ActiveMQConnectionFactory;
import org.activemq.broker.BrokerContainer;
import org.activemq.broker.impl.BrokerContainerImpl;
import junit.framework.TestCase;


import org.activemq.spring.SpringBrokerContainerFactory;
import org.springframework.core.io.ClassPathResource;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.TextMessage;
import javax.jms.Message;

import org.activemq.io.impl.DefaultWireFormat;
import org.activemq.store.PersistenceAdapter;
import org.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.derby.jdbc.EmbeddedDataSource;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class EmbeddedRemoteConnectorTest extends TestCase{
    BrokerContainer receiveBroker;
    String subject = "TOOL.DEFAULT";
     int messageCount = 10;
     boolean init = false;



    protected ActiveMQConnectionFactory createReceiverConnectionFactory() throws JMSException {
        receiveBroker = new BrokerContainerImpl("receiver");
        receiveBroker.addConnector("tcp://localhost:61616");
        receiveBroker.setPersistenceAdapter(createPersistenceAdapter());
        receiveBroker.start();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(receiveBroker, "tcp://localhost:61616");

        return factory;
    }

    protected ActiveMQConnectionFactory createEmbeddedRemoteBrokerConnectionFactory() throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setUseEmbeddedBroker(true);
        connectionFactory.setBrokerContainerFactory(new SpringBrokerContainerFactory(new ClassPathResource("org/activemq/usecases/EmbeddedRemoteConnector.xml")));

        return connectionFactory;
    }




    public void testSendFromEmbeddedRemote() throws Exception {

        ActiveMQConnectionFactory embeddedRemoteFactory = createEmbeddedRemoteBrokerConnectionFactory();
        Connection conn = embeddedRemoteFactory.createConnection();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(subject);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i = 0; i < messageCount; i++) {
           TextMessage message = session.createTextMessage("Message : " +i);
           producer.send(message);
           System.out.println("Sending   " + message.getText());
        }



    }

    public void testReceiver() throws Exception {
        ActiveMQConnectionFactory receiveFactory = createReceiverConnectionFactory();

        Connection conn = receiveFactory.createConnection();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(subject);
        MessageConsumer consumer = session.createConsumer(destination);
        conn.start();

        
        int counter =0;
        
        for (int i = 0; i < messageCount; i++) {
            System.out.println("Before receive");
            Message message = consumer.receive(10000);
            try {
                if (message instanceof TextMessage) {
                   TextMessage txtMsg = (TextMessage) message;
                   System.out.println("Received : " + txtMsg.getText());

                   //increment counter only when a message is received
                   if(txtMsg.getText()!=null && txtMsg.getText().length()==0) {
                       counter++;
                   }


                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }


        }
        
        consumer.close();
        session.close();
        conn.close();
        
        assertTrue("No messages were received",counter==messageCount );

    }



    protected void tearDown() throws Exception {

        if (receiveBroker != null) {
            receiveBroker.stop();
        }
    }






    /**
     * Returns the persistence adapter.
     * Sets up the testing database to be used when the messages are persistent.
     * It attempts to recreate the tables everytime the test is executed.
     *
     * @return PersistenceAdapter - persistence adapter.
     */
    protected PersistenceAdapter createPersistenceAdapter() {
        EmbeddedDataSource ds = new EmbeddedDataSource();
        ds.setDatabaseName("testdb");
        if (!init) {
            ds.setCreateDatabase("create");
        }

        JDBCPersistenceAdapter persistenceAdapter = new JDBCPersistenceAdapter(ds, new DefaultWireFormat());

        if (!init) {
            persistenceAdapter.setDropTablesOnStartup(true);
        }

        init = true;

        return persistenceAdapter;
    }    

}
