package org.activemq.sampler;

import org.apache.jmeter.testelement.TestListener;
import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.log.Logger;
import org.apache.jorphan.logging.LoggingManager;

import org.activemq.util.connection.ServerConnectionFactory;
import org.activemq.util.IdGenerator;

import javax.jms.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ProducerSysTest extends Sampler implements MessageListener {

    private static final Logger log = LoggingManager.getLoggerForClass();

    // Otherwise, the response is scanned for these strings
    private static final String STATUS_PREFIX = JMeterUtils.getPropDefault("tcp.status.prefix", "");
    private static final String STATUS_SUFFIX = JMeterUtils.getPropDefault("tcp.status.suffix", "");
    private static final String STATUS_PROPERTIES = JMeterUtils.getPropDefault("tcp.status.properties", "");

    private static final Properties statusProps = new Properties();

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
     * Constructor for ProducerSysTest Sampler object.
     */
    public ProducerSysTest() {
        log.debug("Created " + this);
        protocolHandler = this.getProtocol(); //from superclass sampler.
        log.debug("Using Protocol Handler: " + protocolHandler.getClass().getName());
    }

    /**
     * Sends the config message for validating the test messages.
     *
     * @throws Exception
     */
    protected void publishConfigMessage()  throws Exception  {
        String text = getConfigMessage();
        boolean topic = false;

        Connection connection = ServerConnectionFactory.createConnectionFactory(this.getURL(),
                                                                                ACTIVEMQ_SERVER,
                                                                                topic,
                                                                                this.getEmbeddedBroker());

        Session session = ServerConnectionFactory.createSession(connection,
                                                                TRANSACTED_FALSE,
                                                                ACTIVEMQ_SERVER,
                                                                topic);

        Destination destination = ServerConnectionFactory.createDestination(session,
                                                                            CONFIG_SUBJECT,
                                                                            this.getURL(),
                                                                            ACTIVEMQ_SERVER,
                                                                            topic);

        MessageProducer publisher = session.createProducer(destination);

        publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        publishConfigMessage(connection, session, publisher, text);
    }

    /**
     * Sends the config information.
     *
     * @param connection
     * @param session
     * @param publisher
     * @param text
     * @throws JMSException
     */
    protected void publishConfigMessage(Connection connection, Session session, MessageProducer publisher, String text)
        throws JMSException {

        Message message = session.createTextMessage(text);
        publisher.send(message);

        // Close the connection and session after sending the config message
        ServerConnectionFactory.close(connection, session);
    }

    /**
     * Creates the publishers then publish the test messages.
     *
     * @throws JMSException
     */
    protected void publish() throws JMSException {
        String subjects[] = getSubjects();

        for( int i=0;i<getNoProducer();i++) {
            final int  x = i;
            final String subject = subjects[i % subjects.length];

            Thread thread = new Thread() {

            public void run() {
              try {
                   publish(x, subject);

              } catch (Exception e) {
                 e.printStackTrace();
              }
            }
            };

            thread.start();
        }
    }

    /**
     *  Sends the the information from the client via tcp.
     *
     * @param x
     * @param subject
     * @throws Exception
     */
    protected void publish(int x, String subject) throws Exception {
        MessageProducer publisher = null;

        Connection connection = ServerConnectionFactory.createConnectionFactory(this.getURL(),
                                                                                ACTIVEMQ_SERVER,
                                                                                this.getTopic(),
                                                                                this.getEmbeddedBroker());

        if (this.getDurable()) {
            IdGenerator idGenerator = new IdGenerator();
            connection.setClientID(idGenerator.generateId());
        }

        Session session = ServerConnectionFactory.createSession(connection,
                                                                TRANSACTED_FALSE,
                                                                ACTIVEMQ_SERVER,
                                                                this.getTopic());

        Destination destination = ServerConnectionFactory.createDestination(session,
                                                                            subject,
                                                                            this.getURL(),
                                                                            ACTIVEMQ_SERVER,
                                                                            this.getTopic());

        publisher = session.createProducer(destination);


        if (getDurable()) {
            publisher.setDeliveryMode(DeliveryMode.PERSISTENT);
        } else {
            publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }

        StringBuffer sb = new StringBuffer();

        for (int i=0; i < this.getNoMessages()-1; ++i){
             sb.append("PROD");
             sb.append(x);
             sb.append("#BODY");
             sb.append("#");
             sb.append(i + 1);

             TextMessage message = session.createTextMessage(sb.toString());
             publisher.send(message);

             sb.delete(0, sb.length());
         }

         sb.append("PROD");
         sb.append(x);
         sb.append("#");
         sb.append(LAST_MESSAGE);
         sb.append("#");
         sb.append(this.getNoMessages());

         TextMessage message = session.createTextMessage(sb.toString());

         publisher.send(message);
    }

    /**
     *  Subscribe to the confirmation message that the consumer received the config message.
     *
     * @throws JMSException
     */
    protected void subscribe() throws JMSException {
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
                                                                this.getMQServer(),
                                                                this.getTopic());

        Destination destination = ServerConnectionFactory.createDestination(session,
                                                                            CONFIRM_SUBJECT,
                                                                            this.getURL(),
                                                                            ACTIVEMQ_SERVER,
                                                                            this.getTopic());

        MessageConsumer consumer = null;


        if (this.getDurable() && this.getTopic()) {
            consumer = session.createDurableSubscriber((Topic) destination, getClass().getName());
        } else {
            consumer = session.createConsumer(destination);
        }

        consumer.setMessageListener(this);
        addResource(consumer);
    }

    /**
     * Runs and publish the message.
     *
     * @throws Exception
     */
    public void run() throws Exception {

        // Publish the config message.
        publishConfigMessage();

        // Wait for the confirmation message
        subscribe();
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
            // Run the benchmark tool code
            this.run();
        } catch (Exception ex) {
            log.debug("Error running producer ", ex);
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

            TextMessage textMessage = (TextMessage) message;

            String confirmMsg = textMessage.getText();

            if (confirmMsg.equals(PUBLISH_MSG)) {
                publish();
            }

        } catch (JMSException e) {
            log.error("Unable to force deserialize the content", e);
        }
    }
}
