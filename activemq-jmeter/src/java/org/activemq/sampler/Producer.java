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

import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestListener;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.activemq.util.connection.ServerConnectionFactory;
import org.apache.activemq.util.IdGenerator;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.Queue;
import javax.jms.TopicSession;
import javax.jms.QueueSession;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.TopicPublisher;
import javax.jms.QueueSender;
import javax.jms.MessageProducer;
import javax.jms.DeliveryMode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A sampler which understands Tcp requests.
 */
public class Producer extends Sampler implements TestListener {

    public static int counter;

    private static final Logger log = LoggingManager.getLoggerForClass();

    // Otherwise, the response is scanned for these strings
    private static final String STATUS_PREFIX = JMeterUtils.getPropDefault("tcp.status.prefix", "");
    private static final String STATUS_SUFFIX = JMeterUtils.getPropDefault("tcp.status.suffix", "");
    private static final String STATUS_PROPERTIES = JMeterUtils.getPropDefault("tcp.status.properties", "");

    private static final Properties statusProps = new Properties();
    private static final long INSECONDS = 60;
    private static final long MSGINTERVALINSECS = 60;

    private Timer timerPublish;
    private Timer timerPublishLoop;

    private int batchCounter = 0;

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
                //haveStatusProps = true;
            } catch (FileNotFoundException e) {
                log.info("Property file not found");
            } catch (IOException e) {
                log.info("Property file error " + e.toString());
            }
        }
    }

    /**
     * Constructor for ProducerSampler object.
     */
    public Producer() {
        log.debug("Created " + this);
        protocolHandler = this.getProtocol(); //from superclass sampler.
        log.debug("Using Protocol Handler: " + protocolHandler.getClass().getName());
    }

    /**
     * Increments the int variable.
     *
     * @param count - variable incremented.
     */
    protected synchronized void count(int count) {
        counter += count;
    }

    /**
     * @return Returns the message.
     */
    protected String getMessage() {
        StringBuffer buffer = new StringBuffer();

        for (int i = 0; i < getMsgSize(); i++) {
            char ch = 'X';
            buffer.append(ch);
        }

        return buffer.toString();
    }

    /**
     * Retrieves the message then sends it via tcp.
     *
     * @throws Exception
     */
    protected void publish() throws Exception {
        long threadRampUp = 0;

        if (getNoProducer() > 0) {
            threadRampUp = (long) ((double) (getRampUp() * INSECONDS) / ((double) getNoProducer()) * 1000);
        }

        timerPublish = new Timer();
        timerPublish.scheduleAtFixedRate(new newThread(), 0, threadRampUp);
    }

    /**
     * Sends the information from the client via tcp.
     *
     * @param text    - message that is sent.
     * @param subject - subject of the message to be sent.
     * @throws JMSException
     */
    protected void publish(String text, String subject) throws JMSException {
        Destination destination = null;
        Session session = null;
        MessageProducer publisher = null;
        Connection connection = null;

        connection = ServerConnectionFactory.createConnectionFactory(this.getURL(),
                                                                     this.getMQServer(),
                                                                     this.getTopic(),
                                                                     this.getEmbeddedBroker());

        if (this.getDurable()) {
            if ((ServerConnectionFactory.JORAM_SERVER.equals(this.getMQServer()))||
                (ServerConnectionFactory.MANTARAY_SERVER.equals(this.getMQServer()))) {
                //Id set be server

            } else {
                IdGenerator idGenerator = new IdGenerator();
                connection.setClientID(idGenerator.generateId());
            }
        }

        session = ServerConnectionFactory.createSession(connection,
                                                        this.getTransacted(),
                                                        this.getMQServer(),
                                                        this.getTopic());

        destination = ServerConnectionFactory.createDestination(session,
                                                                subject,
                                                                this.getURL(),
                                                                this.getMQServer(),
                                                                this.getTopic());

        if ((ServerConnectionFactory.OPENJMS_SERVER.equals(this.getMQServer()))||
            (ServerConnectionFactory.MANTARAY_SERVER.equals(this.getMQServer()))) {
            if (this.getTopic()){
                connection.start();
                TopicPublisher topicPublisher = ((TopicSession)session).createPublisher((Topic)destination);
                publisher = topicPublisher;

            } else {
                connection.start();
                QueueSender queuePublisher = ((QueueSession)session).createSender((Queue)destination);
                publisher = queuePublisher;

            }

        } else {
            publisher = session.createProducer(destination);
        }

        long msgIntervalInMins = this.getMsgInterval();
        long msgIntervalInSecs = msgIntervalInMins * INSECONDS;

        if (msgIntervalInSecs < 0) {
            msgIntervalInSecs = MSGINTERVALINSECS;
        }

        if (getDurable()) {
            publisher.setDeliveryMode(DeliveryMode.PERSISTENT);
        } else {
            publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }

        if (this.getDefMsgInterval()) {
            while (!stopThread) {
                publishLoop(session, publisher, text);
            }
            ServerConnectionFactory.close(connection, session);

        } else {
            // set the session, publisher and connection.
            this.setSession(session);
            this.setPublisher(publisher);
            this.setConnection(connection);

            timerPublishLoop = new Timer();
            timerPublishLoop.scheduleAtFixedRate(new publish(), 0, msgIntervalInSecs * 1000);

        }
    }

    /**
     * Sends a message through MessageProducer object.
     *
     * @param session   - Session oject.
     * @param publisher - MessageProducer object.
     * @param text      - text that is used to create Message object.
     * @throws JMSException
     */
    protected void publishLoop(Session session, MessageProducer publisher, String text) throws JMSException {
        if (ServerConnectionFactory.OPENJMS_SERVER.equals(this.getMQServer())) {
            if (publisher instanceof TopicPublisher) {
                Message message = ((TopicSession)session).createTextMessage(text);
                ((TopicPublisher)publisher).publish(message);
            } else if (publisher instanceof QueueSender){
                Message message = ((QueueSession)session).createTextMessage(text);
                ((QueueSender)publisher).send(message);
            }
        } else {
            Message message = session.createTextMessage(text);
            publisher.send(message);
        }

        if (this.getTransacted()) {
            batchCounter++;

            if (batchCounter == this.getBatchSize()) {
                batchCounter = 0;
                session.commit();
            }
        }

        count(1);
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
     * Runs and publish the message.
     *
     * @throws Exception
     */
    public void run() throws Exception {
        start();
        publish();
    }

    /**
     * Retrieves the sample as SampleResult object. There are times that this
     * is ignored.
     *
     * @param e - Entry object.
     * @return Returns the sample result.
     */
    public SampleResult sample(Entry e) { // Entry tends to be ignored ...
        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());
        res.setSamplerData(getURL());
        res.sampleStart();

        try {
            //run the benchmark tool code
            this.run();
        } catch (Exception ex) {
            log.debug("Error running producer ", ex);
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
     * Logs the end of the test. This is called only once per
     * class.
     */
    public void testEnded() {
        log.debug(this + " test ended");
    }

    /**
     * Logs the host at the end of the test.
     *
     * @param host - the host to be logged.
     */
    public void testEnded(String host) {
        log.debug(this + " test ended on " + host);
    }

    /**
     * Logs the start of the test. This is called only once
     * per class.
     */
    public void testStarted() {
        log.debug(this + " test started");
    }

    /**
     * Logs the host at the start of the test.     *
     * @param host - the host to be logged.
     */
    public void testStarted(String host) {
        log.debug(this + " test started on " + host);
    }

    /**
     * Logs the iteration event.
     *
     * @param event
     */
    public void testIterationStart(LoopIterationEvent event) {
        log.debug(this + " test iteration start on " + event.getIteration());
    }

    /**
     * Creates thread for publishing messages.
     */
    class newThread extends TimerTask {
        final String text = getMessage();
        int numberOfProducer = getNoProducer();
        int counter = 0;

        public void run() {
             if (counter < numberOfProducer) {
                 final String subject = subjects[counter % getNoSubject()];

                 counter++;

                 Thread thread = new Thread() {
                    public void run() {
                        try {
                              if (stopThread) {
                                  return;
                              } else {
                                  publish(text, subject);
                              }
                        } catch (JMSException e) {
                            log.error("Error publishing message ", e);
                        }
                    }
                };

                thread.start();

             } else {
                 timerPublish.cancel();
             }
        }
    }

    /**
     * Starts the publish loop timer.
     */
    class publish extends TimerTask {
        public void run() {
            try {
                if (!stopThread) {
                    publishLoop(getSession(), getPublisher(), getMessage());
                } else {
                    ServerConnectionFactory.close(getConnection(), getSession());
                    timerPublishLoop.cancel();
                }
            } catch(JMSException e) {
                log.error("Could not publish "+e);
            }
        }
    }

    /**
     * Starts an instance of the Producer tool.
     */
    public static void main(String[] args) {
        System.out.println("##########################################");
        System.out.println(" Producer * start *");
        System.out.println("##########################################");

        Producer prod = new Producer();

        if (args.length == 0 ){
            displayToolParameters();
        }

        if (args.length > 0){
            String mqServer = args[0];

            if (mqServer.equalsIgnoreCase("SONICMQ")){
                prod.setMQServer(ServerConnectionFactory.SONICMQ_SERVER);
            } else if (mqServer.equalsIgnoreCase("TIBCOMQ")) {
                prod.setMQServer(ServerConnectionFactory.TIBCOMQ_SERVER);
            } else if (mqServer.equalsIgnoreCase("JBOSSMQ")) {
                prod.setMQServer(ServerConnectionFactory.JBOSSMQ_SERVER);
            } else if (mqServer.equalsIgnoreCase("OPENJMS")) {
                prod.setMQServer(ServerConnectionFactory.OPENJMS_SERVER);
            } else if (mqServer.equalsIgnoreCase("JORAM")) {
                prod.setMQServer(ServerConnectionFactory.JORAM_SERVER);
            } else if (mqServer.equalsIgnoreCase("MANTARAY")) {
                prod.setMQServer(ServerConnectionFactory.MANTARAY_SERVER);
            } else if (mqServer.equalsIgnoreCase("ACTIVEMQ")) {
                //Run with the default broker
            } else {
                System.out.print("Please enter a valid server: [ ");
                System.out.print("SONICMQ | " );
                System.out.print("TIBCOMQ | " );
                System.out.print("JBOSSMQ | " );
                System.out.print("OPENJMS | " );
                System.out.print("JORAM |");
                System.out.print("MANTARAY |");
                System.out.println("ACTIVEMQ ]");
            }

        }

        if (args.length > 1) {
            prod.setURL(args[1]);
        } else {
            System.out.println("Please specify the URL.");
        }

        if (args.length > 2) {
            prod.setDuration(args[2]);
        }

        if (args.length > 3) {
            prod.setRampUp(args[3]);
        }

        if (args.length > 4) {
            prod.setNoProducer(args[4]);
        }

        if (args.length > 5) {
            prod.setNoSubject(args[5]);
        }

        if (args.length > 6) {
            prod.setMsgSize(args[6]);
        }

        if (args.length > 7) {
            prod.setDurable(args[7]);
        }

        if (args.length > 8) {
            prod.setTopic(args[8]);
        }

        if (args.length > 9) {
            prod.setTransacted(args[9]);

            if (args.length > 10) {
                prod.setBatchSize(args[10]);
            } else {
                displayToolParameters();
                System.out.println("Please specify the batch size.");
                return;
            }
        }

        if (args.length > 11) {
            prod.setDefMsgInterval(args[11]);
            if (!prod.getDefMsgInterval()) {
                if (args.length > 12) {
                    prod.setMsgInterval(args[12]);
                } else {
                    displayToolParameters();
                    System.out.println("Please specify the message interval.");
                    return;
                 }
             }
        }

        prod.setDefMsgInterval("true");

        System.out.println("Runnning Consumer tool with the following parameters:");
        System.out.println("Server=" + prod.getMQServer());
        System.out.println("URL="+prod.getURL());
        System.out.println("Duration="+prod.getDuration());
        System.out.println("Ramp up="+prod.getRampUp());
        System.out.println("No. Producer="+prod.getNoProducer());
        System.out.println("No. Subject="+prod.getNoSubject());
        System.out.println("Msg Size="+prod.getMsgSize());
        System.out.println("Is Durable="+prod.getDurable());
        System.out.println("Is Topic="+prod.getTopic());
        System.out.println("Is Transacted="+prod.getTransacted());

        try {
            prod.run();
        } catch (Exception e){
            System.out.println("Excception e="+e);

        }
        System.out.println("##########################################");
        System.out.println("Producer * end *");
        System.out.println("##########################################");
    }

    /**
     * Prints to the console the Producer tool parameters.
     */
    private static void displayToolParameters(){
        System.out.println("Producer tool usage: ");
        System.out.print("[Message Queue Server] ");
        System.out.print("[URL] ");
        System.out.print("[Duration] ");
        System.out.print("[Ramp up] ");
        System.out.print("[No. of producer] ");
        System.out.print("[No. of subject] ");
        System.out.print("[Message size] ");
        System.out.print("[Delivery mode] ");
        System.out.print("[Is topic] ");
        System.out.print("[Is transacted] ");
        System.out.print("[Batch size] ");
        System.out.print("[Has Message interval] ");
        System.out.println("[Message interval] ");
    }
}