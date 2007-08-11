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

package org.apache.activemq.soaktest;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.tool.Producer;
import org.apache.activemq.tool.Consumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;

import junit.framework.TestCase;

import java.io.File;


public class SoakTestSupport  extends TestCase{

    private static final Log log = LogFactory.getLog(SoakTestSupport.class);
    protected BrokerService broker;
    protected String brokerURL = "tcp://localhost:61616";
    protected int consumerCount = 1;
    protected int producerCount = 1;
    protected int messageSize = 1024;
    protected int messageCount = 1000;

    protected Producer[] producers;
    protected Consumer[] consumers;
    protected String destinationName = "TOOL.DEFAULT";
    protected Message payload;

    protected ConnectionFactory connectionFactory;
    protected Destination destination;
    protected boolean createConnectionPerClient = true;
    protected boolean topic = false;
    protected boolean transacted = false;
    protected boolean durable = true;
    protected boolean useEmbeddedBroker = true;
    protected boolean keepOnRunning = true;
    protected int duration = 0;   //duration in minutes
    protected boolean useConsumerListener = true;
    protected Consumer allMessagesList = new Consumer();
    private String dataFileRoot =  "activemq-data";


    protected void setUp() throws Exception {
        //clean up db store
        File dataFile = new File(dataFileRoot);
        recursiveDelete(dataFile);

        if (useEmbeddedBroker) {
            if (broker == null) {
                broker = createBroker();
            }
        }

        connectionFactory = createConnectionFactory();
        Connection con = connectionFactory.createConnection();
        Session session = con.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

        if (topic) {
            destination = session.createTopic(destinationName);
        } else {
            destination = session.createQueue(destinationName);
        }

        createPayload(session);

        con.close();

    }


    protected void createPayload(Session session) throws JMSException {

        byte[] array = new byte[messageSize];
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) i;
        }

        BytesMessage bystePayload = session.createBytesMessage();
        bystePayload.writeBytes(array);
        payload = (Message) bystePayload;
    }


    protected void createProducers() throws JMSException {
        producers = new Producer[producerCount];
        for (int i = 0; i < producerCount; i++) {
            producers[i] = new Producer(connectionFactory, destination);
            if (durable) {
                producers[i].setDeliveryMode(DeliveryMode.PERSISTENT);
            }
             else {
                producers[i].setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            producers[i].start();
        }

    }

    protected void createConsumers() throws JMSException {
        consumers = new Consumer[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            consumers[i] = new Consumer(connectionFactory, destination);
            consumers[i].setParent(allMessagesList);
            if(useConsumerListener){
               consumers[i].start();
            }


        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws JMSException {

        if (useEmbeddedBroker) {
            return new ActiveMQConnectionFactory("vm://localhost");
        } else {
            return new ActiveMQConnectionFactory(brokerURL);
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        configureBroker(broker);
        broker.start();
        return broker;
    }

    protected void configureBroker(BrokerService broker) throws Exception {
        broker.addConnector("vm://localhost");
        broker.setDeleteAllMessagesOnStartup(true);
    }

    public void startTimer() {

        Thread timer = new Thread(new Runnable() {
            public void run() {
                try {

                    Thread.sleep(duration * 60 * 1000);
                    keepOnRunning = true;
                } catch (InterruptedException e) {

                } finally {

                }
            }
        }, "TimerThread");


        log.info("Starting timer thread... Duration :" +duration + " minutes");
        timer.start();
    }

    protected  void recursiveDelete(File file) {
        if( file.isDirectory() ) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        file.delete();
    }
}
