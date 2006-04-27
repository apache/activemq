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
package org.activemq.sampler;

import org.apache.activemq.util.IdGenerator;
import org.activemq.util.connection.ServerConnectionFactory;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import org.mr.MantaAgent;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.Queue;
import javax.jms.TopicSession;
import javax.jms.QueueSession;
import javax.jms.Destination;
import javax.jms.MessageListener;
import javax.jms.QueueReceiver;
import javax.jms.MessageConsumer;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Consumer extends Sampler implements MessageListener {

    public static int counter;

    private static final Logger log = LoggingManager.getLoggerForClass();

    // Otherwise, the response is scanned for these strings
    private static final String STATUS_PREFIX = JMeterUtils.getPropDefault("tcp.status.prefix", "");
    private static final String STATUS_SUFFIX = JMeterUtils.getPropDefault("tcp.status.suffix", "");

    private static final String STATUS_PROPERTIES = JMeterUtils.getPropDefault("tcp.status.properties", "");
    private static final Properties statusProps = new Properties();

    private int batchCounter = 0;

    private static long ID = 0;
    static {
        log.info("Protocol Handler name=" + getClassname());
        log.info("Status prefix=" + STATUS_PREFIX);
        log.info("Status suffix=" + STATUS_SUFFIX);
        log.info("Status properties=" + STATUS_PROPERTIES);

        if (STATUS_PROPERTIES.length() > 0) {
            File f = new File(STATUS_PROPERTIES);

            try {
                statusProps.load(new FileInputStream(f));
                log.info("Successfully loaded properties");
            } catch (FileNotFoundException e) {
                log.error("Property file not found");
            } catch (IOException e) {
                log.error("Property file error " + e.toString());
            }
        }
    }

    /**
     * Constructor for ConsumerSampler object.
     */
    public Consumer() {
        log.debug("Created " + this);
        protocolHandler = getProtocol();
        log.debug("Using Protocol Handler: " + protocolHandler.getClass().getName());
    }

    /**
     * Increments the int variable.
     *
     * @param count - variable incremented.
     */
    private synchronized void count(int count) {
        counter += count;
    }

    /**
     * @return the current number of messages sent.
     */
    public static synchronized int resetCount() {
        int answer = counter;
        counter = 0;
        return answer;
    }


    /**
     * Subscribes the subject.
     *
     * @throws JMSException
     */
    protected void subscribe() throws JMSException {
        for (int i = 0; i < getNoConsumer(); i++) {
            String subject = subjects[i % getNoSubject()];
            subscribe(subject);
        }
    }

    /**
     * Subscribes the message.
     *
     * @param subject - subject to be subscribed.
     * @throws JMSException
     */
    protected void subscribe(String subject) throws JMSException {
        Connection connection = ServerConnectionFactory.createConnectionFactory(this.getURL(),
                                                                                this.getMQServer(),
                                                                                this.getTopic(),
                                                                                this.getAsyncSend());

        if (this.getDurable()) {
            if ((ServerConnectionFactory.JORAM_SERVER.equals(this.getMQServer())) ||
                (ServerConnectionFactory.MANTARAY_SERVER.equals(this.getMQServer()))) {
                //Id set by server
            } else {
                if ((ServerConnectionFactory.SWIFTMQ_SERVER.equals(this.getMQServer()))) {
                    ID += 1;
                    String strId = String.valueOf(ID);
                    connection.setClientID(strId);
                } else {
                    IdGenerator idGenerator = new IdGenerator();
                    connection.setClientID(idGenerator.generateId());
                }
            }
        }

        //start connection before receiving messages.
//        connection.start();
        Session session = ServerConnectionFactory.createSession(connection,
                                                                this.getTransacted(),
                                                                this.getMQServer(),
                                                                this.getTopic());

        Destination destination = ServerConnectionFactory.createDestination(session,
                                                                            subject,
                                                                            this.getURL(),
                                                                            this.getMQServer(),
                                                                            this.getTopic());

        MessageConsumer consumer = null;
        connection.start();

        if (ServerConnectionFactory.OPENJMS_SERVER.equals(this.getMQServer())) {
            if (this.getTopic()) {
                Topic topic = (Topic) destination;

                if (this.getDurable()) {
                    consumer = ((TopicSession) session).createDurableSubscriber(topic,
                                   getClass().getName());
                } else {
                    consumer = ((TopicSession) session).createSubscriber(topic);
                }

            } else {
                Queue queue = ((QueueSession) session).createQueue(subject);
                QueueReceiver receiver = ((QueueSession) session).createReceiver(queue);
                consumer = (MessageConsumer) receiver;
            }
        } else if (ServerConnectionFactory.MANTARAY_SERVER.equals(this.getMQServer())) {
            if (this.getTopic()) {
                Topic topic = (Topic) destination;

                if (this.getDurable()) {
                    consumer = ((TopicSession) session).createDurableSubscriber(topic,
                                   MantaAgent.getInstance().getAgentName());
                } else {
                    consumer = ((TopicSession) session).createSubscriber(topic);
                }

            } else {
                Queue queue = ((QueueSession) session).createQueue(subject);
                QueueReceiver receiver = ((QueueSession) session).createReceiver(queue);
                consumer = (MessageConsumer) receiver;
            }
        } else if (ServerConnectionFactory.SWIFTMQ_SERVER.equals(this.getMQServer())) {
            if (this.getTopic()) {
                Topic topic = (Topic) destination;
                if (this.getDurable()) {
                    consumer = ((TopicSession) session).createDurableSubscriber(topic,"durableSubscriber");
                } else {
                    consumer = ((TopicSession) session).createSubscriber(topic);
                }
            } else {
                subject = "testqueue@router1";
                Queue queue = ((QueueSession) session).createQueue(subject);
                QueueReceiver receiver = ((QueueSession) session).createReceiver(queue);
                consumer = (MessageConsumer) receiver;
            }
        } else {
            if (this.getDurable() && this.getTopic()) {
                consumer = session.createDurableSubscriber((Topic) destination, getClass().getName());
            } else {
                consumer = session.createConsumer(destination);
            }
        }

        this.setSession(session);
        consumer.setMessageListener(this);
        addResource(consumer);
    }

    /**
     *  Processes the received message.
     *
     * @param message - message received by the listener.
     */
    public void onMessage(Message message) {
        try {
            TextMessage textMessage = (TextMessage) message;
            Session session;
            // lets force the content to be deserialized
            String text = textMessage.getText();
            count(1);

            if (this.getTransacted()) {
                batchCounter++;
                if (batchCounter == this.getBatchSize()) {
                    batchCounter = 0;
                    session = this.getSession();
                    session.commit();
                }
            }
        } catch (JMSException e) {
            log.error("Unable to force deserialize the content ", e);
        }
    }

    /**
     * Runs and subscribes to messages.
     *
     * @throws JMSException
     */
    public void run() throws JMSException {
        start();
        subscribe();
    }

    /**
     * Retrieves the sample as SampleResult object. There are times that this
     * is ignored.
     *
     * @param e - Entry object.
     * @return Returns the sample result.
     */
    public SampleResult sample(Entry e) {// Entry tends to be ignored ...
        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());
        res.setSamplerData(getURL());
        res.sampleStart();

        try {
            this.run();
        } catch (JMSException ex) {
            log.error("Error running consumer ", ex);
            res.setResponseCode("500");
            res.setResponseMessage(ex.toString());
        }

        //Calculate response time
        res.sampleEnd();

        // Set if we were successful or not
        res.setSuccessful(true);

        return res;
    }

    /**
     * Starts an instance of the Consumer tool.
     */
    public static void main(String args[]) {
        System.out.println("##########################################");
        System.out.println(" Consumer * start *");
        System.out.println("##########################################");

        Consumer cons = new Consumer();

        if (args.length == 0) {
            displayToolParameters();
        }

        if (args.length > 0) {
            String mqServer = args[0];

            if (mqServer.equalsIgnoreCase("SONICMQ")) {
                cons.setMQServer(ServerConnectionFactory.SONICMQ_SERVER);
            } else if (mqServer.equalsIgnoreCase("TIBCOMQ")) {
                cons.setMQServer(ServerConnectionFactory.TIBCOMQ_SERVER);
            } else if (mqServer.equalsIgnoreCase("JBOSSMQ")) {
                cons.setMQServer(ServerConnectionFactory.JBOSSMQ_SERVER);
            } else if (mqServer.equalsIgnoreCase("OPENJMS")) {
                cons.setMQServer(ServerConnectionFactory.OPENJMS_SERVER);
            } else if (mqServer.equalsIgnoreCase("JORAM")) {
                cons.setMQServer(ServerConnectionFactory.JORAM_SERVER);
            } else if (mqServer.equalsIgnoreCase("MANTARAY")) {
                cons.setMQServer(ServerConnectionFactory.MANTARAY_SERVER);
            } else if (mqServer.equalsIgnoreCase("SWIFTMQ")) {
                cons.setMQServer(ServerConnectionFactory.SWIFTMQ_SERVER);
            } else if (mqServer.equalsIgnoreCase("ACTIVEMQ")) {
                //Run with the default broker
            } else {
                System.out.print("Please enter a valid mq server: [ ");
                System.out.print("SONICMQ | ");
                System.out.print("TIBCOMQ | ");
                System.out.print("JBOSSMQ | ");
                System.out.print("OPENJMS | ");
                System.out.print("JORAM | ");
                System.out.print("MANTARAY |");
                System.out.print("SWIFTMQ |");
                System.out.println("ACTIVEMQ ]");
                return;
            }

        }

        if (args.length > 1) {
            cons.setURL(args[1]);
        }

        if (args.length > 2) {
            cons.setDuration(args[2]);
        }

        if (args.length > 3) {
            cons.setRampUp(args[3]);
        }

        if (args.length > 4) {
            cons.setNoConsumer(args[4]);
        }

        if (args.length > 5) {
            cons.setNoSubject(args[5]);
        }

        if (args.length > 6) {
            cons.setDurable(args[6]);
        }

        if (args.length > 7) {
            cons.setTopic(args[7]);
        }

        if (args.length > 8) {
            cons.setTransacted(args[8]);

            if (cons.getTransacted()) {
                if (args.length > 9) {
                    cons.setBatchSize(args[9]);
                } else {
                    displayToolParameters();
                    System.out.println("Please specify the batch size.");
                    return;
                }
            }
        }
        System.out.println("Runnning Consumer tool with the following parameters:");
        System.out.println("Server=" + cons.getMQServer());
        System.out.println("URL=" + cons.getURL());
        System.out.println("Duration=" + cons.getDuration());
        System.out.println("Ramp up=" + cons.getRampUp());
        System.out.println("No consumer=" + cons.getNoConsumer());
        System.out.println("No Subject=" + cons.getNoSubject());
        System.out.println("Is Durable=" + cons.getDurable());
        System.out.println("Is Topic=" + cons.getTopic());
        System.out.println("Is Transacted=" + cons.getTransacted());
        System.out.println("Batch size=" + cons.getBatchSize());

        try {
            cons.run();
        } catch (Exception e) {
            System.out.println("Excception e=" + e);

        }

        System.out.println("##########################################");
        System.out.println(" Consumer * end *");
        System.out.println("##########################################");

    }

    /**
     * Prints to the console the Consumer tool parameters.
     */
    private static void displayToolParameters() {
        System.out.println(" Consumer tool usage: ");
        System.out.print("[Message Queue Server] ");
        System.out.print("[URL] ");
        System.out.print("[Duration] ");
        System.out.print("[Ramp up] ");
        System.out.print("[No. of consumer] ");
        System.out.print("[No. of subject] ");
        System.out.print("[Delivery mode] ");
        System.out.print("[Is topic] ");
        System.out.print("[Is transacted] ");
        System.out.print("[Batch size] ");
    }
}