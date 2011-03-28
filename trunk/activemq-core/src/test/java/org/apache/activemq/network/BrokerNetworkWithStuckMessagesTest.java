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
package org.apache.activemq.network;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageNotWriteableException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.Wait;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class duplicates most of the functionality in {@link NetworkTestSupport} 
 * and {@link BrokerTestSupport} because more control was needed over how brokers 
 * and connectors are created. Also, this test asserts message counts via JMX on 
 * each broker. 
 * 
 * @author bsnyder
 *
 */
public class BrokerNetworkWithStuckMessagesTest extends TestCase /*NetworkTestSupport*/ {
    
    private static final Logger LOG = LoggerFactory.getLogger(BrokerNetworkWithStuckMessagesTest.class);
    
    private BrokerService localBroker; 
    private BrokerService remoteBroker; 
    private DemandForwardingBridge bridge;
    
    protected Map<String, BrokerService> brokers = new HashMap<String, BrokerService>();
    protected ArrayList connections = new ArrayList();
    
    protected TransportConnector connector;
    protected TransportConnector remoteConnector;
    
    protected long idGenerator;
    protected int msgIdGenerator;
    protected int tempDestGenerator;
    protected int maxWait = 4000;
    protected String queueName = "TEST";
    
    protected String amqDomain = "org.apache.activemq";
        
    protected void setUp() throws Exception {
        
        // For those who want visual confirmation: 
        //   Uncomment the following to enable JMX support on a port number to use 
        //   Jconsole to view each broker. You will need to add some calls to 
        //   Thread.sleep() to be able to actually slow things down so that you 
        //   can manually see JMX attrs. 
//        System.setProperty("com.sun.management.jmxremote", "");
//        System.setProperty("com.sun.management.jmxremote.port", "1099");
//        System.setProperty("com.sun.management.jmxremote.authenticate", "false");
//        System.setProperty("com.sun.management.jmxremote.ssl", "false");
        
        // Create the local broker 
        createBroker();
        // Create the remote broker 
        createRemoteBroker();
        
        // Remove the activemq-data directory from the creation of the remote broker
        FileUtils.deleteDirectory(new File("activemq-data"));
        
        // Create a network bridge between the local and remote brokers so that 
        // demand-based forwarding can take place
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        config.setBrokerName("local");
        config.setDispatchAsync(false);
        
        Transport localTransport = createTransport(); 
        Transport remoteTransport = createRemoteTransport();
        
        // Create a network bridge between the two brokers 
        bridge = new DemandForwardingBridge(config, localTransport, remoteTransport);
        bridge.setBrokerService(localBroker);
        bridge.start();
        
        waitForBridgeFormation();
        
    }
    
    protected void waitForBridgeFormation() throws Exception {
        for (final BrokerService broker : brokers.values()) {
            if (!broker.getNetworkConnectors().isEmpty()) {
            	// Max wait here is 30 secs
                Wait.waitFor(new Wait.Condition() {
                    public boolean isSatisified() throws Exception {
                        return !broker.getNetworkConnectors().get(0).activeBridges().isEmpty();
                    }});
            }
        }
    }
    
    protected void tearDown() throws Exception {
        bridge.stop();
        localBroker.stop();
        remoteBroker.stop();
    }

    public void testBrokerNetworkWithStuckMessages() throws Exception {
        
        int sendNumMessages = 10;
        int receiveNumMessages = 5;
        
        // Create a producer 
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);
        
        // Create a destination on the local broker 
        ActiveMQDestination destinationInfo1 = null;
        
        // Send a 10 messages to the local broker 
        for (int i = 0; i < sendNumMessages; ++i) {
            destinationInfo1 = createDestinationInfo(connection1, connectionInfo1, ActiveMQDestination.QUEUE_TYPE);
            connection1.request(createMessage(producerInfo, destinationInfo1, DeliveryMode.NON_PERSISTENT));
        }
        
        // Ensure that there are 10 messages on the local broker 
        Object[] messages = browseQueueWithJmx(localBroker);
        assertEquals(sendNumMessages, messages.length);
        
        
        // Create a synchronous consumer on the remote broker 
        final StubConnection connection2 = createRemoteConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        ActiveMQDestination destinationInfo2 = 
            createDestinationInfo(connection2, connectionInfo2, ActiveMQDestination.QUEUE_TYPE);
        final ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destinationInfo2);
        connection2.send(consumerInfo2);
        
        // Consume 5 of the messages from the remote broker and ack them. 
        // Because the prefetch size is set to 1000 in the createConsumerInfo() 
        // method, this will cause the messages on the local broker to be 
        // forwarded to the remote broker. 
        for (int i = 0; i < receiveNumMessages; ++i) {
            Message message1 = receiveMessage(connection2);
            assertNotNull(message1);
            connection2.send(createAck(consumerInfo2, message1, 1, MessageAck.STANDARD_ACK_TYPE));
            
            Object[] msgs1 = browseQueueWithJmx(remoteBroker);
            LOG.info("Found [" + msgs1.length + "] messages with JMX");
//            assertEquals((sendNumMessages-i), msgs.length);
        }
        
        // Ensure that there are zero messages on the local broker. This tells 
        // us that those messages have been prefetched to the remote broker 
        // where the demand exists. 
        messages = browseQueueWithJmx(localBroker);
        assertEquals(0, messages.length);
        
        // Close the consumer on the remote broker 
        connection2.send(consumerInfo2.createRemoveCommand());
        
        // There should now be 5 messages stuck on the remote broker 
        messages = browseQueueWithJmx(remoteBroker);
        assertEquals(5, messages.length);
        
        // Create a consumer on the local broker just to confirm that it doesn't 
        // receive any messages  
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destinationInfo1);
        connection1.send(consumerInfo1);
        Message message1 = receiveMessage(connection1);
        
        //////////////////////////////////////////////////////
        // An assertNull() is done here because this is currently the correct 
        // behavior. This is actually the purpose of this test - to prove that 
        // messages are stuck on the remote broker. AMQ-2324 and AMQ-2484 aim 
        // to fix this situation so that messages don't get stuck. 
        assertNull(message1);
        //////////////////////////////////////////////////////
        
        ConsumerInfo consumerInfo3 = createConsumerInfo(sessionInfo2, destinationInfo2);
        connection2.send(consumerInfo3);
        
        // Consume the last 5 messages from the remote broker and ack them just 
        // to clean up the queue. 
        int counter = 0;
        for (int i = 0; i < receiveNumMessages; ++i) {
            message1 = receiveMessage(connection2);
            assertNotNull(message1);
            connection2.send(createAck(consumerInfo3, message1, 1, MessageAck.STANDARD_ACK_TYPE));
            ++counter;
        }
        // Ensure that 5 messages were received
        assertEquals(receiveNumMessages, counter);
        
        // Let those acks percolate... This stinks but it's the only way currently
        // because these types of internal broker actions are non-deterministic. 
        Thread.sleep(4000);
        
        // Ensure that the queue on the remote broker is empty 
        messages = browseQueueWithJmx(remoteBroker);
        assertEquals(0, messages.length);
        
        // Close the consumer on the remote broker 
        connection2.send(consumerInfo3.createRemoveCommand());
        
        connection1.stop();
        connection2.stop();
    }
    
    protected BrokerService createBroker() throws Exception {
        localBroker = new BrokerService(); 
        localBroker.setBrokerName("localhost");
        localBroker.setUseJmx(true);
        localBroker.setPersistenceAdapter(null);
        localBroker.setPersistent(false);
        connector = createConnector();
        localBroker.addConnector(connector);
        localBroker.start();
        localBroker.waitUntilStarted();
        
        localBroker.getManagementContext().setConnectorPort(2221);
        
        brokers.put(localBroker.getBrokerName(), localBroker);
        
        return localBroker;
    }
    
    protected BrokerService createRemoteBroker() throws Exception {
        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("remotehost");
        remoteBroker.setUseJmx(true);
        remoteBroker.setPersistenceAdapter(null);
        remoteBroker.setPersistent(false);
        remoteConnector = createRemoteConnector();
        remoteBroker.addConnector(remoteConnector);
        remoteBroker.waitUntilStarted();
        
        remoteBroker.getManagementContext().setConnectorPort(2222);
        
        brokers.put(remoteBroker.getBrokerName(), remoteBroker);
        
        return remoteBroker;
    }
    
    protected Transport createTransport() throws Exception {
        Transport transport = TransportFactory.connect(connector.getServer().getConnectURI());
        return transport;
    }
    
    protected Transport createRemoteTransport() throws Exception {
        Transport transport = TransportFactory.connect(remoteConnector.getServer().getConnectURI());
        return transport;
    }
    
    protected TransportConnector createConnector() throws Exception, IOException, URISyntaxException {
        return new TransportConnector(TransportFactory.bind(new URI(getLocalURI())));
    }
    
    protected TransportConnector createRemoteConnector() throws Exception, IOException, URISyntaxException {
        return new TransportConnector(TransportFactory.bind(new URI(getRemoteURI())));
    }
    
    protected String getRemoteURI() {
        return "vm://remotehost";
    }

    protected String getLocalURI() {
        return "vm://localhost";
    }
    
    protected StubConnection createConnection() throws Exception {
        Transport transport = TransportFactory.connect(connector.getServer().getConnectURI());
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

    protected StubConnection createRemoteConnection() throws Exception {
        Transport transport = TransportFactory.connect(remoteConnector.getServer().getConnectURI());
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }
    
    @SuppressWarnings("unchecked")
    private Object[] browseQueueWithJms(BrokerService broker) throws Exception {
		Object[] messages = null;
		Connection connection = null;
		Session session = null;

		try {
			URI brokerUri = connector.getUri();
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUri.toString());
			connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue destination = session.createQueue(queueName);
			QueueBrowser browser = session.createBrowser(destination);
			List<Message> list = new ArrayList<Message>();
			for (Enumeration<Message> enumn = browser.getEnumeration(); enumn.hasMoreElements();) {
				list.add(enumn.nextElement());
			}
			messages = list.toArray();
		}
		finally {
			if (session != null) {
				session.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
		LOG.info("+Browsed with JMS: " + messages.length);
		
		return messages;
	}
    
    private Object[] browseQueueWithJmx(BrokerService broker) throws Exception {
        Hashtable<String, String> params = new Hashtable<String, String>();
        params.put("BrokerName", broker.getBrokerName());
        params.put("Type", "Queue");
        params.put("Destination", queueName);
        ObjectName queueObjectName = ObjectName.getInstance(amqDomain, params);
        
        ManagementContext mgmtCtx = broker.getManagementContext();
        QueueViewMBean queueView = (QueueViewMBean)mgmtCtx.newProxyInstance(queueObjectName, QueueViewMBean.class, true);
        
        Object[] messages = (Object[]) queueView.browse();
        
		LOG.info("+Browsed with JMX: " + messages.length);
        
        return messages;
    }
    
    protected ConnectionInfo createConnectionInfo() throws Exception {
        ConnectionInfo info = new ConnectionInfo();
        info.setConnectionId(new ConnectionId("connection:" + (++idGenerator)));
        info.setClientId(info.getConnectionId().getValue());
        return info;
    }
    
    protected SessionInfo createSessionInfo(ConnectionInfo connectionInfo) throws Exception {
        SessionInfo info = new SessionInfo(connectionInfo, ++idGenerator);
        return info;
    }
    
    protected ProducerInfo createProducerInfo(SessionInfo sessionInfo) throws Exception {
        ProducerInfo info = new ProducerInfo(sessionInfo, ++idGenerator);
        return info;
    }
    
    protected ConsumerInfo createConsumerInfo(SessionInfo sessionInfo, ActiveMQDestination destination) throws Exception {
        ConsumerInfo info = new ConsumerInfo(sessionInfo, ++idGenerator);
        info.setBrowser(false);
        info.setDestination(destination);
        info.setPrefetchSize(1000);
        info.setDispatchAsync(false);
        return info;
    }
    
    protected DestinationInfo createTempDestinationInfo(ConnectionInfo connectionInfo, byte destinationType) {
        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(connectionInfo.getConnectionId());
        info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
        info.setDestination(ActiveMQDestination.createDestination(info.getConnectionId() + ":" + (++tempDestGenerator), destinationType));
        return info;
    }
    
    protected ActiveMQDestination createDestinationInfo(StubConnection connection, ConnectionInfo connectionInfo1, byte destinationType) throws Exception {
        if ((destinationType & ActiveMQDestination.TEMP_MASK) != 0) {
            DestinationInfo info = createTempDestinationInfo(connectionInfo1, destinationType);
            connection.send(info);
            return info.getDestination();
        } else {
            return ActiveMQDestination.createDestination(queueName, destinationType);
        }
    }
    
    protected Message createMessage(ProducerInfo producerInfo, ActiveMQDestination destination, int deliveryMode) {
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(deliveryMode == DeliveryMode.PERSISTENT);
        return message;
    }
    
    protected Message createMessage(ProducerInfo producerInfo, ActiveMQDestination destination) {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setMessageId(new MessageId(producerInfo, ++msgIdGenerator));
        message.setDestination(destination);
        message.setPersistent(false);
        try {
            message.setText("Test Message Payload.");
        } catch (MessageNotWriteableException e) {
        }
        return message;
    }

    protected MessageAck createAck(ConsumerInfo consumerInfo, Message msg, int count, byte ackType) {
        MessageAck ack = new MessageAck();
        ack.setAckType(ackType);
        ack.setConsumerId(consumerInfo.getConsumerId());
        ack.setDestination(msg.getDestination());
        ack.setLastMessageId(msg.getMessageId());
        ack.setMessageCount(count);
        return ack;
    }
    
    public Message receiveMessage(StubConnection connection) throws InterruptedException {
        return receiveMessage(connection, maxWait);
    }

    public Message receiveMessage(StubConnection connection, long timeout) throws InterruptedException {
        while (true) {
            Object o = connection.getDispatchQueue().poll(timeout, TimeUnit.MILLISECONDS);

            if (o == null) {
                return null;
            }
            if (o instanceof MessageDispatch) {

                MessageDispatch dispatch = (MessageDispatch)o;
                if (dispatch.getMessage() == null) {
                    return null;
                }
                dispatch.setMessage(dispatch.getMessage().copy());
                dispatch.getMessage().setRedeliveryCounter(dispatch.getRedeliveryCounter());
                return dispatch.getMessage();
            }
        }
    }
    
}
