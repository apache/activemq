package org.apache.activemq.broker;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.CompositeDataConstants;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class UserIDBrokerTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(UserIDBrokerTest.class);

    private static final String DESTINATION_NAME = "TEST.RECONNECT";
    private static final String LOCAL_BROKER_TRANSPORT_URI = "tcp://localhost:61616";
    private static final String LOCAL_USER = "localuser";
    private static final String NC = "NC";
    private static final String PASSWORD = "supersecret";
    private static final String REMOTE_BROKER_TRANSPORT_URI = "tcp://localhost:61617";
    private static final String REMOTE_USER = "remoteuser";
    private static final String TEXT_MESSAGE = "This is a message!";

    private BrokerService localBroker;
    private BrokerService remoteBroker;
    private MessageProducer localProducer;
    private Session localSession;

    private final ActiveMQQueue destination = new ActiveMQQueue(DESTINATION_NAME);


    @Override
    protected void setUp() throws Exception {
        LOG.info("Setting up LocalBroker");
        localBroker = new BrokerService();
        localBroker.setBrokerName("LocalBroker");
        localBroker.setUseJmx(false);
        localBroker.setPersistent(false);
        localBroker.setTransportConnectorURIs(new String[]{LOCAL_BROKER_TRANSPORT_URI});
    }

    public void testSend_ShouldNotSetJmsxUserId_WhenNotEnabled() throws Exception {
        localBroker.setPopulateJMSXUserID(false);
        startBroker(localBroker);
        startLocalConnection();

        MessageConsumer localConsumer = localSession.createConsumer(destination);

        javax.jms.Message message = localSession.createTextMessage(DESTINATION_NAME + ": " + TEXT_MESSAGE);
        localProducer.send(message);

        message = localConsumer.receive(10000);
        assertNotNull(message);
        assertNull(message.getStringProperty(CompositeDataConstants.JMSXUSER_ID));
    }

    public void testSend_ShouldSetJmsxUserId_WhenEnabled() throws Exception {
        localBroker.setPopulateJMSXUserID(true);
        startBroker(localBroker);
        startLocalConnection();

        MessageConsumer localConsumer = localSession.createConsumer(destination);

        javax.jms.Message message = localSession.createTextMessage(DESTINATION_NAME + ": " + TEXT_MESSAGE);
        localProducer.send(message);

        message = localConsumer.receive(10000);
        assertNotNull(message);
        String user = message.getStringProperty(CompositeDataConstants.JMSXUSER_ID);
        assertEquals(LOCAL_USER, user);
    }


    public void testSend_ShouldNotOverrideJmsxUserId_WhenEnabledAndNetworkConnection() throws Exception {
        localBroker.setPopulateJMSXUserID(true);
        startBroker(localBroker);
        startLocalConnection();

        LOG.info("Setting up RemoteBroker");
        remoteBroker = new BrokerService();
        remoteBroker.setPopulateJMSXUserID(true);
        remoteBroker.setBrokerName("RemoteBroker");
        remoteBroker.setUseJmx(false);
        remoteBroker.setPersistent(false);
        remoteBroker.setTransportConnectorURIs(new String[]{REMOTE_BROKER_TRANSPORT_URI});
        startBroker(remoteBroker);

        LOG.info("Adding network connector 'NC1'...");
        NetworkConnector nc = localBroker.addNetworkConnector("static:(" + REMOTE_BROKER_TRANSPORT_URI + ")");
        nc.setName(NC);
        nc.setRemoteUserName(REMOTE_USER);
        nc.start();
        assertTrue(nc.isStarted());

        LOG.info("Looking up network connector by name...");
        NetworkConnector nc1 = localBroker.getNetworkConnectorByName(NC);
        assertNotNull("Should find network connector", nc1);
        assertTrue(nc1.isStarted());
        assertEquals(nc, nc1);

        ActiveMQConnectionFactory remoteFactory = new ActiveMQConnectionFactory(REMOTE_USER, PASSWORD, REMOTE_BROKER_TRANSPORT_URI);
        Connection remoteConnection = remoteFactory.createConnection();
        remoteConnection.start();
        Session remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer remoteConsumer = remoteSession.createConsumer(destination);

        Message message = localSession.createTextMessage("test");
        localProducer.send(message);

        message = remoteConsumer.receive(10000);
        assertNotNull(message);
        String user = message.getStringProperty(CompositeDataConstants.JMSXUSER_ID);
        assertEquals(LOCAL_USER, user);
    }

    @Override
    protected void tearDown() throws Exception {
        stopBroker(localBroker);
        stopBroker(remoteBroker);
    }

    private void startBroker(BrokerService localBroker) throws Exception {
        localBroker.start();
        localBroker.waitUntilStarted();
    }

    private void startLocalConnection() throws JMSException {
        ActiveMQConnectionFactory localFactory = new ActiveMQConnectionFactory(LOCAL_USER, PASSWORD, LOCAL_BROKER_TRANSPORT_URI);
        Connection localConnection = localFactory.createConnection();
        localConnection.start();
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        localProducer = localSession.createProducer(destination);
    }

    private void stopBroker(BrokerService brokerService) throws Exception {
        if (brokerService != null && brokerService.isStarted()) {
            LOG.info("Stopping LocalBroker");
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }
}