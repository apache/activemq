package org.apache.activemq.transport.failover;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

public class InitalReconnectDelayTest {

    private static final transient Logger LOG = LoggerFactory.getLogger(InitalReconnectDelayTest.class);
    protected BrokerService broker1;
    protected BrokerService broker2;
    protected CountDownLatch broker2Started = new CountDownLatch(1);
    protected String uriString = "failover://(tcp://localhost:62001,tcp://localhost:62002)?randomize=false&initialReconnectDelay=15000";

    @Test
    public void testInitialReconnectDelay() throws Exception {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uriString);
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue("foo");
        MessageProducer producer = session.createProducer(destination);

        long start = (new Date()).getTime();
        producer.send(session.createTextMessage("TEST"));
        long end = (new Date()).getTime();

        //Verify we can send quickly
        assertTrue((end - start) < 2000);

        //Halt the broker1...
        LOG.info("Stopping the Broker1...");
        broker1.stop();

        LOG.info("Attempting to send... failover should kick in...");
        start = (new Date()).getTime();
        producer.send(session.createTextMessage("TEST"));
        end = (new Date()).getTime();

        //Inital reconnection should kick in and be darned close to what we expected
        LOG.info("Failover took " + (end - start) + " ms.");
        assertTrue("Failover took " + (end - start) + " ms and should be > 14000.", (end - start) > 14000);

    }

    @Before
    public void setUp() throws Exception {

        final String dataDir = "target/data/shared";

        broker1 = new BrokerService();

        broker1.setBrokerName("broker1");
        broker1.setDeleteAllMessagesOnStartup(true);
        broker1.setDataDirectory(dataDir);
        broker1.addConnector("tcp://localhost:62001");
        broker1.setUseJmx(false);
        broker1.start();
        broker1.waitUntilStarted();

        broker2 = new BrokerService();
        broker2.setBrokerName("broker2");
        broker2.setDataDirectory(dataDir);
        broker2.setUseJmx(false);
        broker2.addConnector("tcp://localhost:62002");
        broker2.start();
        broker2.waitUntilStarted();

    }

    protected String getSlaveXml() {
        return "org/apache/activemq/broker/ft/sharedFileSlave.xml";
    }

    protected String getMasterXml() {
        return "org/apache/activemq/broker/ft/sharedFileMaster.xml";
    }

    @After
    public void tearDown() throws Exception {

        if (broker1.isStarted()) {
            broker1.stop();
            broker1.waitUntilStopped();
        }

        if (broker2.isStarted()) {
            broker2.stop();
            broker2.waitUntilStopped();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(uriString);
    }

}
