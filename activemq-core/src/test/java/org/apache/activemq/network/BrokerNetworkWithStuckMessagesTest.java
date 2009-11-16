package org.apache.activemq.network;

import javax.jms.DeliveryMode;

import junit.framework.Test;

import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BrokerNetworkWithStuckMessagesTest extends NetworkTestSupport {
	
    private static final Log LOG = LogFactory.getLog(BrokerNetworkWithStuckMessagesTest.class);
	
	private DemandForwardingBridge bridge;

	protected void setUp() throws Exception {
        super.setUp();
        
        // Create a network bridge between the local and remote brokers so that 
        // demand-based forwarding can take place
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        config.setBrokerName("local");
        config.setDispatchAsync(false);
        bridge = new DemandForwardingBridge(config, createTransport(), createRemoteTransport());
        bridge.setBrokerService(broker);
        bridge.start();
        
        // Enable JMX support on the local and remote brokers 
        broker.setUseJmx(true);
        remoteBroker.setUseJmx(true);
        
        // Set the names of teh local and remote brokers 
        broker.setBrokerName("local");
        remoteBroker.setBrokerName("remote");
    }
	
	protected void tearDown() throws Exception {
        bridge.stop();
        super.tearDown();
    }

	public void testBrokerNetworkWithStuckMessages() throws Exception {
		
		int sendNumMessages = 10;
		int receiveNumMessages = 5;
		
		// Create a producer and send a batch of 10 messages to the local broker
		StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);
        
        // Create a destination on the local broker 
        ActiveMQDestination destinationInfo1 = null;
        
        for (int i = 0; i < sendNumMessages; ++i) {
        	destinationInfo1 = createDestinationInfo(connection1, connectionInfo1, ActiveMQDestination.QUEUE_TYPE);
	        connection1.send(createMessage(producerInfo, destinationInfo1, DeliveryMode.NON_PERSISTENT));
        }
        
        // Ensure that there are 10 messages on the local broker 
        assertTrue(countMessagesInQueue(connection1, connectionInfo1, destinationInfo1) == 10);
        
        
        // Create a consumer on the remote broker 
        StubConnection connection2 = createRemoteConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        ActiveMQDestination destinationInfo2 = 
        	createDestinationInfo(connection2, connectionInfo2, ActiveMQDestination.QUEUE_TYPE);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destinationInfo2);
        connection2.send(consumerInfo2);
        
        // Consume 5 of the messages from the remote broker and ack them. 
        // Because the prefetch size is set to 1000, this will cause the 
        // messages on the local broker to be forwarded to the remote broker. 
        for (int i = 0; i < receiveNumMessages; ++i) {
	        Message message1 = receiveMessage(connection2);
	        assertNotNull(message1);
            connection2.send(createAck(consumerInfo2, message1, 1, MessageAck.STANDARD_ACK_TYPE));
        }
        
        // Close the consumer on the remote broker 
        connection2.send(consumerInfo2.createRemoveCommand());
        
        // Ensure that there are zero messages on the local broker. This tells 
        // us that those messages have been prefetched to the remote broker 
        // where the demand exists. 
        assertTrue(countMessagesInQueue(connection1, connectionInfo1, destinationInfo1) == 0);
        
        // There should now be 5 messages stuck on the remote broker 
        assertTrue(countMessagesInQueue(connection2, connectionInfo2, destinationInfo1) == 5);
        
        // Create a consumer on the local broker just to confirm that it doesn't 
        // receive any messages  
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destinationInfo1);
        connection1.send(consumerInfo1);
        Message message1 = receiveMessage(connection1);
        
		//////////////////////////////////////////////////////
        // An assertNull() is done here because this is currently the correct 
        // behavior. This is actually the purpose of this test - to prove that 
        // messages are stuck on the remote broker. AMQ-2324 aims to fix this 
        // situation so that messages don't get stuck. 
        assertNull(message1);
		//////////////////////////////////////////////////////
        
        consumerInfo2 = createConsumerInfo(sessionInfo2, destinationInfo2);
        connection2.send(consumerInfo2);
        
        // Consume the last 5 messages from the remote broker and ack them just 
        // to clean up the queue. 
        for (int i = 0; i < receiveNumMessages; ++i) {
	        message1 = receiveMessage(connection2);
	        assertNotNull(message1);
            connection2.send(createAck(consumerInfo2, message1, 1, MessageAck.STANDARD_ACK_TYPE));
        }
        
        // Close the consumer on the remote broker 
        connection2.send(consumerInfo2.createRemoveCommand());
        
        // Ensure that the queue on the remote broker is empty 
        assertTrue(countMessagesInQueue(connection2, connectionInfo2, destinationInfo2) == 0);
	}
	
    public static Test suite() {
        return suite(BrokerNetworkWithStuckMessagesTest.class);
    }
    
}
