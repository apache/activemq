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

package org.apache.activemq.tool;

import java.util.Properties;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JMSMemtest {

    private static final Logger LOG = LoggerFactory.getLogger(JMSMemtest.class);
    private static final int DEFAULT_MESSAGECOUNT = 5000;
    
    protected BrokerService broker;
    protected boolean topic = true;
    protected boolean durable;
    protected long messageCount;

    //  how large the message in kb before we close/start the producer/consumer with a new connection.  -1 means no connectionCheckpointSize
    protected int connectionCheckpointSize;
    protected long connectionInterval;


    protected int consumerCount;
    protected int producerCount;
    protected int checkpointInterval;
    protected int prefetchSize;
    //set 10 kb of payload as default
    protected int messageSize;

    protected String reportDirectory;
    protected String reportName;


    protected String url = "";
    protected MemProducer[] producers;
    protected MemConsumer[] consumers;
    protected String destinationName;
    protected boolean allMessagesConsumed = true;
    protected MemConsumer allMessagesList = new MemConsumer();

    protected Message payload;

    protected ActiveMQConnectionFactory connectionFactory;
    protected Connection connection;
    protected Destination destination;


    protected boolean createConnectionPerClient = true;

    protected boolean transacted;
    protected boolean useEmbeddedBroker = true;
    protected MemoryMonitoringTool memoryMonitoringTool;

    public JMSMemtest(Properties settings) {
        url = settings.getProperty("url");
        topic = Boolean.parseBoolean(settings.getProperty("topic"));
        durable = Boolean.parseBoolean(settings.getProperty("durable"));
        connectionCheckpointSize = Integer.valueOf(settings.getProperty("connectionCheckpointSize"));
        producerCount = Integer.valueOf(settings.getProperty("producerCount"));
        consumerCount = Integer.valueOf(settings.getProperty("consumerCount"));
        messageCount = Integer.valueOf(settings.getProperty("messageCount"));
        messageSize = Integer.valueOf(settings.getProperty("messageSize"));
        prefetchSize = Integer.valueOf(settings.getProperty("prefetchSize"));
        checkpointInterval = Integer.valueOf(settings.getProperty("checkpointInterval")) * 1000;
        producerCount = Integer.valueOf(settings.getProperty("producerCount"));
        reportName = settings.getProperty("reportName");
        destinationName = settings.getProperty("destinationName");
        reportDirectory = settings.getProperty("reportDirectory");
        connectionInterval = connectionCheckpointSize * 1024L;
    }

    public static void main(String[] args) {


        Properties sysSettings = new Properties();

        for (int i = 0; i < args.length; i++) {

            int index = args[i].indexOf("=");
            String key = args[i].substring(0, index);
            String val = args[i].substring(index + 1);
            sysSettings.setProperty(key, val);

        }


        JMSMemtest memtest = new JMSMemtest(sysSettings);
        try {
            memtest.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    protected void start() throws Exception {
        LOG.info("Starting Monitor");
        memoryMonitoringTool = new MemoryMonitoringTool();
        memoryMonitoringTool.setTestSettings(getSysTestSettings());
        Thread monitorThread = memoryMonitoringTool.startMonitor();

        if (messageCount == 0) {
            messageCount = DEFAULT_MESSAGECOUNT;
        }


        if (useEmbeddedBroker) {
            if (broker == null) {
                broker = createBroker();
            }
        }


        connectionFactory = (ActiveMQConnectionFactory) createConnectionFactory();
        if (prefetchSize > 0) {
            connectionFactory.getPrefetchPolicy().setTopicPrefetch(prefetchSize);
            connectionFactory.getPrefetchPolicy().setQueuePrefetch(prefetchSize);
        }

        connection = connectionFactory.createConnection();
        Session session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

        if (topic) {
            destination = session.createTopic(destinationName);
        } else {
            destination = session.createQueue(destinationName);
        }

        createPayload(session);

        publishAndConsume();

        LOG.info("Closing resources");
        this.close();

        monitorThread.join();


    }


    protected boolean resetConnection(int counter) {
        if (connectionInterval > 0) {
            long totalMsgSizeConsumed = counter * 1024L;
            if (connectionInterval < totalMsgSizeConsumed) {
                return true;
            }
        }
        return false;
    }

    protected void publishAndConsume() throws Exception {

        createConsumers();
        createProducers();
        int counter = 0;
        boolean resetCon = false;
        LOG.info("Start sending messages ");
        for (int i = 0; i < messageCount; i++) {
            if (resetCon) {
                closeConsumers();
                closeProducers();
                createConsumers();
                createProducers();
                resetCon = false;
            }

            for (int k = 0; k < producers.length; k++) {
                producers[k].sendMessage(payload, "counter", counter);
                counter++;
                if (resetConnection(counter)) {
                    resetCon = true;
                    break;
                }
            }
        }
    }


    protected void close() throws Exception {
        connection.close();
        broker.stop();

        memoryMonitoringTool.stopMonitor();
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
        producers = new MemProducer[producerCount];
        for (int i = 0; i < producerCount; i++) {
            producers[i] = new MemProducer(connectionFactory, destination);
            if (durable) {
                producers[i].setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
                producers[i].setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            producers[i].start();
        }

    }

    protected void createConsumers() throws JMSException {
        consumers = new MemConsumer[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            consumers[i] = new MemConsumer(connectionFactory, destination);
            consumers[i].setParent(allMessagesList);
            consumers[i].start();


        }
    }

    protected void closeProducers() throws JMSException {
        for (int i = 0; i < producerCount; i++) {
            producers[i].shutDown();
        }

    }

    protected void closeConsumers() throws JMSException {
        for (int i = 0; i < consumerCount; i++) {
            consumers[i].shutDown();
        }
    }

    protected ConnectionFactory createConnectionFactory() throws JMSException {

        if (url == null || url.trim().equals("") || url.trim().equals("null")) {
            return new ActiveMQConnectionFactory("vm://localhost");
        } else {
            return new ActiveMQConnectionFactory(url);
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

    protected Properties getSysTestSettings() {
        Properties settings = new Properties();
        settings.setProperty("domain", topic ? "topic" : "queue");
        settings.setProperty("durable", durable ? "durable" : "non-durable");
        settings.setProperty("connection_checkpoint_size_kb", Integer.valueOf(connectionCheckpointSize).toString());
        settings.setProperty("producer_count", Integer.valueOf(producerCount).toString());
        settings.setProperty("consumer_count", Integer.valueOf(consumerCount).toString());
        settings.setProperty("message_count", Long.valueOf(messageCount).toString());
        settings.setProperty("message_size", Integer.valueOf(messageSize).toString());
        settings.setProperty("prefetchSize", Integer.valueOf(prefetchSize).toString());
        settings.setProperty("checkpoint_interval", Integer.valueOf(checkpointInterval).toString());
        settings.setProperty("destination_name", destinationName);
        settings.setProperty("report_name", reportName);
        settings.setProperty("report_directory", reportDirectory);
        settings.setProperty("connection_checkpoint_size", Integer.valueOf(connectionCheckpointSize).toString());
        return settings;
    }


}
