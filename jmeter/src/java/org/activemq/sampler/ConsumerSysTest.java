/**
 *
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.log.Logger;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.Entry;
import org.activemq.util.connection.ServerConnectionFactory;
import org.activemq.util.IdGenerator;
import org.activemq.command.ActiveMQMessage;

import javax.jms.*;
import java.util.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;



public class ConsumerSysTest extends Sampler implements MessageListener {

    private static final Logger log = LoggingManager.getLoggerForClass();

    // Otherwise, the response is scanned for these strings
    private static final String STATUS_PREFIX = JMeterUtils.getPropDefault("tcp.status.prefix", "");
    private static final String STATUS_SUFFIX = JMeterUtils.getPropDefault("tcp.status.suffix", "");

    private static final String STATUS_PROPERTIES = JMeterUtils.getPropDefault("tcp.status.properties", "");
    private static final Properties statusProps = new Properties();
    private static int msgCounter;

    public static int noOfMessages;
    public static int ConsumerCount;
    public static Map ProducerMap = Collections.synchronizedMap(new ConcurrentHashMap());
    public static Map CopyProducerMap = Collections.synchronizedMap(new ConcurrentHashMap());
    public static boolean destination;
    public static boolean resetMap = false;

    private MessageConsumer consumer = null;

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
     *  Constructor for ConsumerSampler object.
     */
    public ConsumerSysTest() {
        log.debug("Created " + this);
        protocolHandler = getProtocol();
        log.debug("Using Protocol Handler: " + protocolHandler.getClass().getName());
    }

    /**
     *  Subscribe to the config message.
     *
     * @throws JMSException
     */
    protected void suscribeConfigMessage() throws JMSException {
        boolean topic = false;

        Connection connection = ServerConnectionFactory.createConnectionFactory(this.getURL(),
                                                                                ACTIVEMQ_SERVER,
                                                                                topic,
                                                                                this.getEmbeddedBroker());

        // Start connection before receiving messages.
        connection.start();

        Session session = ServerConnectionFactory.createSession(connection,
                                                                TRANSACTED_FALSE,
                                                                ACTIVEMQ_SERVER,
                                                                topic);

        Destination destination = ServerConnectionFactory.createDestination(session,
                                                                            CONFIG_SUBJECT,
                                                                            this.getURL(),
                                                                            ACTIVEMQ_SERVER,
                                                                            topic);

        MessageConsumer consumer = null;
        consumer = session.createConsumer(destination);
        Message message = consumer.receive();

        TextMessage txtMsg = (TextMessage) message;
        String configMsg = txtMsg.getText();

        noOfMessages = Integer.parseInt(configMsg.substring(configMsg.indexOf("#")+1, configMsg.lastIndexOf("#")));

        ServerConnectionFactory.close(connection, session);
    }

    /**
     *  Create the subscriber/s then subscribe.
     *
     * @throws JMSException
     */
    protected void subscribe() throws JMSException {
        String subjects[] = getSubjects();

        for (int i = 0; i < this.getNoConsumer(); i++) {
            String subject = subjects[i % getNoSubject()];
            subscribe(subject);
        }
        ConsumerCount = getNoConsumer();
    }

    /**
     *  Subscribe to the subject.
     *
     * @param subject
     * @throws JMSException
     */
    protected void subscribe(String subject) throws JMSException {
        destination(this.getTopic());
        Connection connection = ServerConnectionFactory.createConnectionFactory(this.getURL(),
                                                                                ACTIVEMQ_SERVER,
                                                                                this.getTopic(),
                                                                                this.getEmbeddedBroker());

        if (this.getDurable()) {
            IdGenerator idGenerator = new IdGenerator();
            connection.setClientID(idGenerator.generateId());
        }

        // Start connection before receiving messages.
        connection.start();

        Session session = ServerConnectionFactory.createSession(connection,
                                                                TRANSACTED_FALSE,
                                                                ACTIVEMQ_SERVER,
                                                                this.getTopic());

        Destination destination = ServerConnectionFactory.createDestination(session,
                                                                            subject,
                                                                            this.getURL(),
                                                                            ACTIVEMQ_SERVER,
                                                                            this.getTopic());


        if (this.getDurable() && this.getTopic()) {
            consumer = session.createDurableSubscriber((Topic) destination, getClass().getName());
        } else {
            consumer = session.createConsumer(destination);
        }

        consumer.setMessageListener(this);
        addResource(consumer);
    }

    /**
     *  Create the publisher then send the confirmation message.
     *
     * @throws JMSException
     */
    protected void publishConfirmMessage() throws JMSException {
        MessageProducer publisher = null;
        String text = PUBLISH_MSG;
        Connection connection = ServerConnectionFactory.createConnectionFactory(this.getURL(),
                                                                                ACTIVEMQ_SERVER,
                                                                                this.getTopic(),
                                                                                this.getEmbeddedBroker());
        if (this.getDurable()) {
            IdGenerator idGenerator = new IdGenerator();
            connection.setClientID(idGenerator.generateId());
        }

        Session session = ServerConnectionFactory.createSession(connection,
                                                                this.getTransacted(),
                                                                ACTIVEMQ_SERVER,
                                                                this.getTopic());

         Destination destination = ServerConnectionFactory.createDestination(session,
                                                                            CONFIRM_SUBJECT,
                                                                            this.getURL(),
                                                                            ACTIVEMQ_SERVER,
                                                                            this.getTopic());

        publisher = session.createProducer(destination);

         if (getDurable()) {
             publisher.setDeliveryMode(DeliveryMode.PERSISTENT);
         } else {
             publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         }

        publishConfirmMessage(connection, session, publisher, text);
    }

    /**
     * publish the confirmation message.
     *
     * @param connection
     * @param session
     * @param publisher
     * @param text
     * @throws JMSException
     */
    protected void publishConfirmMessage(Connection connection, Session session, MessageProducer publisher, String text)
        throws JMSException {

        Message message = session.createTextMessage(text);
        publisher.send(message);

        // Close the connection and session after sending the config message
        ServerConnectionFactory.close(connection, session);
    }

    /**
     * Runs and subscribes to messages.
     *
     * @throws JMSException
     */
    public void run() throws JMSException {

        // Receives the config message
        suscribeConfigMessage();

        // Create subscriber
        subscribe();

        // Publish confirm messages
        publishConfirmMessage();
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

        // Calculate response time
        res.sampleEnd();

        // Set if we were successful or not
        res.setSuccessful(true);

        return res;
    }

    public void onMessage(Message message) {
        try {
            ActiveMQMessage amsg = (ActiveMQMessage) message;
            TextMessage textMessage = (TextMessage) message;

            StringBuffer sb = new StringBuffer();
            sb.append(textMessage.getText());
            sb.append("#");
            sb.append(amsg.getJMSMessageID());

            addToMap(sb.toString());

        } catch (JMSException e) {
            log.error("Unable to force deserialize the content", e);
        }
    }

    /**
     *
     * @param text Add the message to a Producer hash map.
     */
    private synchronized void addToMap(String text) {
       msgCounter++;
       String strMsgCounter = String.valueOf(msgCounter);
       ProducerMap.put(strMsgCounter, text);
    }

    /**
     *
     * @return Resets the Producer map.
     */
    public synchronized  Map resetProducerMap() {
        Map copy = Collections.synchronizedMap(new ConcurrentHashMap(ProducerMap));
        ProducerMap.clear();
        msgCounter = 0;
        return copy;
    }

    /**
     *
     * @param dest
     */
    private void destination(boolean dest) {
        destination = dest;
    }


}