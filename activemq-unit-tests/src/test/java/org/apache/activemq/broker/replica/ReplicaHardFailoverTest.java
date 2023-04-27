package org.apache.activemq.broker.replica;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.jmx.ReplicationViewMBean;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ReplicaHardFailoverTest extends ReplicaPluginTestSupport {

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;
    private ReplicationViewMBean firstBrokerReplicationView;
    private ReplicationViewMBean secondBrokerReplicationView;
    protected static String SECOND_REPLICA_BINDING_ADDRESS = "tcp://localhost:61611";

    @Override
    protected void setUp() throws Exception {
        firstBroker = setUpFirstBroker();
        secondBroker = setUpSecondBroker();

        ReplicaPlugin firstBrokerPlugin = new ReplicaPlugin();
        firstBrokerPlugin.setRole(ReplicaRole.source);
        firstBrokerPlugin.setTransportConnectorUri(firstReplicaBindAddress);
        firstBrokerPlugin.setOtherBrokerUri(SECOND_REPLICA_BINDING_ADDRESS);
        firstBrokerPlugin.setControlWebConsoleAccess(false);
        firstBroker.setPlugins(new BrokerPlugin[]{firstBrokerPlugin});

        ReplicaPlugin secondBrokerPlugin = new ReplicaPlugin();
        secondBrokerPlugin.setRole(ReplicaRole.replica);
        secondBrokerPlugin.setTransportConnectorUri(SECOND_REPLICA_BINDING_ADDRESS);
        secondBrokerPlugin.setOtherBrokerUri(firstReplicaBindAddress);
        secondBrokerPlugin.setControlWebConsoleAccess(false);
        secondBroker.setPlugins(new BrokerPlugin[]{secondBrokerPlugin});

        firstBroker.start();
        secondBroker.start();
        firstBroker.waitUntilStarted();
        secondBroker.waitUntilStarted();

        firstBrokerReplicationView = getReplicationView(firstBroker);
        secondBrokerReplicationView = getReplicationView(secondBroker);
        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);

        destination = createDestination();

    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        super.tearDown();
    }

    @Test
    public void testGetReplicationRoleViaJMX() throws Exception {
        firstBrokerReplicationView = getReplicationView(firstBroker);
        secondBrokerReplicationView = getReplicationView(secondBroker);

        assertEquals(ReplicaRole.source, ReplicaRole.valueOf(firstBrokerReplicationView.getReplicationRole()));
        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(secondBrokerReplicationView.getReplicationRole()));
    }

    @Test
    public void testHardFailover() throws Exception {
        firstBrokerReplicationView.setReplicationRole(ReplicaRole.replica.name(), true);
        secondBrokerReplicationView.setReplicationRole(ReplicaRole.source.name(), true);
        Thread.sleep(LONG_TIMEOUT);

        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(firstBrokerReplicationView.getReplicationRole()));
        assertEquals(ReplicaRole.source, ReplicaRole.valueOf(secondBrokerReplicationView.getReplicationRole()));

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        secondBrokerProducer.send(message);
        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testBothBrokerFailoverToPrimary() throws Exception {
        secondBrokerReplicationView.setReplicationRole(ReplicaRole.source.name(), true);
        Thread.sleep(LONG_TIMEOUT);

        assertEquals(ReplicaRole.source, ReplicaRole.valueOf(firstBrokerReplicationView.getReplicationRole()));
        assertEquals(ReplicaRole.source, ReplicaRole.valueOf(secondBrokerReplicationView.getReplicationRole()));

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        ActiveMQDestination destination2 = new ActiveMQQueue(getDestinationString() + "No2");

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination2);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination2);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        secondBrokerProducer.send(message);
        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testBothBrokerFailoverToReplica() throws Exception {
        firstBrokerReplicationView.setReplicationRole(ReplicaRole.replica.name(), true);
        Thread.sleep(LONG_TIMEOUT);

        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(firstBrokerReplicationView.getReplicationRole()));
        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(secondBrokerReplicationView.getReplicationRole()));

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        ActiveMQDestination destination2 = new ActiveMQQueue(getDestinationString() + "No2");

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination2);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination2);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        secondBrokerProducer.send(message);
        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    private BrokerService setUpSecondBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(secondBindAddress);
        answer.setDataDirectory(SECOND_KAHADB_DIRECTORY);
        answer.setBrokerName("secondBroker");
        return answer;
    }

    private BrokerService setUpFirstBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(firstBindAddress);
        answer.setDataDirectory(FIRST_KAHADB_DIRECTORY);
        answer.setBrokerName("firstBroker");
        return answer;
    }

}
