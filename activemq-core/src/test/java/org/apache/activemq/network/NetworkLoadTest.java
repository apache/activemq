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

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.NoSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This test case is used to load test store and forwarding between brokers.  It sets up
 * n brokers to which have a chain of queues which this test consumes and produces to. 
 * 
 * If the network bridges gets stuck at any point subsequent queues will not get messages.  This test 
 * samples the production and consumption stats every second and if the flow of messages
 * get stuck then this tast fails.  The test monitors the flow of messages for 1 min.
 *  
 * @author chirino
 */
public class NetworkLoadTest extends TestCase {

	private static final transient Log LOG = LogFactory.getLog(NetworkLoadTest.class);

	// How many times do we sample?
    private static final long SAMPLES = Integer.parseInt(System.getProperty("SAMPLES", ""+60*1/5)); 
    // Slower machines might need to make this bigger.
    private static final long SAMPLE_DURATION = Integer.parseInt(System.getProperty("SAMPLES_DURATION", "" + 1000 * 5));
	protected static final int BROKER_COUNT = 4;
	protected static final int MESSAGE_SIZE = 2000;
        String groupId;
        
	class ForwardingClient {

		private final AtomicLong forwardCounter = new AtomicLong();
		private final Connection toConnection;
		private final Connection fromConnection;

		public ForwardingClient(int from, int to) throws JMSException {
			toConnection = createConnection(from);
			Session toSession = toConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			final MessageProducer producer = toSession.createProducer(new ActiveMQQueue("Q"+to));
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			producer.setDisableMessageID(true);

			fromConnection = createConnection(from);
			Session fromSession = fromConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageConsumer consumer = fromSession.createConsumer(new ActiveMQQueue("Q"+from));
			
			consumer.setMessageListener(new MessageListener() {
					public void onMessage(Message msg) {
						try {
							producer.send(msg);
							forwardCounter.incrementAndGet();
						} catch (JMSException e) {
							// this is caused by the connection getting closed. 
						}
					}
			});
		}

		public void start() throws JMSException {
			toConnection.start();
			fromConnection.start();
		}
		
		public void stop() throws JMSException {
		        toConnection.stop();
			fromConnection.stop();
		}
		
		public void close() throws JMSException {
			toConnection.close();
			fromConnection.close();
		}
	}

	private BrokerService[] brokers;
	private ForwardingClient[] forwardingClients;

	
	protected void setUp() throws Exception {
	        groupId = "network-load-test-"+System.currentTimeMillis();
		brokers = new BrokerService[BROKER_COUNT];
		for (int i = 0; i < brokers.length; i++) {
		    LOG.info("Starting broker: "+i);
			brokers[i] = createBroker(i);
			brokers[i].start();
		}
		
		// Wait for the network connection to get setup.
		// The wait is exponential since every broker has to connect to every other broker.
		Thread.sleep(BROKER_COUNT*BROKER_COUNT*50);
		
		forwardingClients = new ForwardingClient[BROKER_COUNT-1];		
		for (int i = 0; i < forwardingClients.length; i++) {
		    LOG.info("Starting fowarding client "+i);
			forwardingClients[i] = new ForwardingClient(i, i+1);
			forwardingClients[i].start();
		}
	}

	protected void tearDown() throws Exception {
		for (int i = 0; i < forwardingClients.length; i++) {
		    LOG.info("Stoping fowarding client "+i);
			forwardingClients[i].close();
		}
		for (int i = 0; i < brokers.length; i++) {
		    LOG.info("Stoping broker "+i);
			brokers[i].stop();
		}
	}

	protected Connection createConnection(int brokerId) throws JMSException {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:"+(60000+brokerId));
        connectionFactory.setOptimizedMessageDispatch(true);
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setUseCompression(false);
        connectionFactory.setDispatchAsync(true);
        connectionFactory.setUseAsyncSend(false);
        connectionFactory.setOptimizeAcknowledge(false);
        connectionFactory.setWatchTopicAdvisories(false);
        ActiveMQPrefetchPolicy qPrefetchPolicy= new ActiveMQPrefetchPolicy();
        qPrefetchPolicy.setQueuePrefetch(100);
        qPrefetchPolicy.setTopicPrefetch(1000);
        connectionFactory.setPrefetchPolicy(qPrefetchPolicy);
        connectionFactory.setAlwaysSyncSend(true);
		return connectionFactory.createConnection();
	}

	protected BrokerService createBroker(int brokerId) throws Exception {
		BrokerService broker = new BrokerService();
		broker.setBrokerName("broker-" + brokerId);
		broker.setPersistent(false);
		broker.setUseJmx(true);
		broker.getManagementContext().setCreateConnector(false);

		final SystemUsage memoryManager = new SystemUsage();
		memoryManager.getMemoryUsage().setLimit(1024 * 1024 * 50); // 50 MB
		broker.setSystemUsage(memoryManager);

		final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
		final PolicyEntry entry = new PolicyEntry();
		entry.setQueue(">");
		entry.setMemoryLimit(1024 * 1024 * 1); // Set to 1 MB
		entry.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
		entry.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
		policyEntries.add(entry);

		// This is to turn of the default behavior of storing topic messages for retroactive consumption
		final PolicyEntry topicPolicyEntry = new PolicyEntry();
		topicPolicyEntry.setTopic(">");
		final NoSubscriptionRecoveryPolicy noSubscriptionRecoveryPolicy = new NoSubscriptionRecoveryPolicy();
		topicPolicyEntry.setSubscriptionRecoveryPolicy(noSubscriptionRecoveryPolicy);

		final PolicyMap policyMap = new PolicyMap();
		policyMap.setPolicyEntries(policyEntries);
		broker.setDestinationPolicy(policyMap);
		
        TransportConnector transportConnector = new TransportConnector();
        transportConnector.setUri(new URI("tcp://localhost:"+(60000+brokerId)));
        
        transportConnector.setDiscoveryUri(new URI("multicast://"+groupId));        
        broker.addConnector(transportConnector);
                        
        DiscoveryNetworkConnector networkConnector = new DiscoveryNetworkConnector();
        networkConnector.setUri(new URI("multicast://"+groupId));
	    networkConnector.setBridgeTempDestinations(true);
	    networkConnector.setPrefetchSize(1);
	    broker.addNetworkConnector(networkConnector);
        
		return broker;
	}
	
	public void testRequestReply() throws Exception {

		final int to = 0; // Send to the first broker
		int from = brokers.length-1; // consume from the last broker..
				
	    LOG.info("Staring Final Consumer");

	    Connection fromConnection = createConnection(from);
		fromConnection.start();
		Session fromSession = fromConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageConsumer consumer = fromSession.createConsumer(new ActiveMQQueue("Q"+from));
		
		final AtomicReference<ActiveMQTextMessage> lastMessageReceived = new AtomicReference<ActiveMQTextMessage>();
		final AtomicLong producedMessages = new AtomicLong();
		final AtomicLong receivedMessages = new AtomicLong();
		final AtomicBoolean done = new AtomicBoolean();

		// Setup the consumer..
		consumer.setMessageListener(new MessageListener() {
			public void onMessage(Message msg) {
				ActiveMQTextMessage m = (ActiveMQTextMessage) msg;
				ActiveMQTextMessage last = lastMessageReceived.get();
				if( last!=null ) {
					// Some order checking...
					if( last.getMessageId().getProducerSequenceId() > m.getMessageId().getProducerSequenceId() ) {
						System.out.println("Received an out of order message. Got "+m.getMessageId()+", expected something after "+last.getMessageId());
					}
				}
				lastMessageReceived.set(m);
				receivedMessages.incrementAndGet();
			}
		});

	    LOG.info("Staring Initial Producer");
		final Connection toConnection = createConnection(to);
		Thread producer = new Thread("Producer") {
			@Override
			public void run() {
				try {
					toConnection.start();
					Session toSession = toConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					final MessageProducer producer = toSession.createProducer(new ActiveMQQueue("Q"+to));
					producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
					producer.setDisableMessageID(true);

					for (int i = 0; !done.get(); i++) {
						TextMessage msg = toSession.createTextMessage(createMessageText(i));
						producer.send(msg);
						producedMessages.incrementAndGet();
					}
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
			
		    private String createMessageText(int index) {
				StringBuffer buffer = new StringBuffer(MESSAGE_SIZE);
				buffer.append(index + " on " + new Date() + " ...");
				if (buffer.length() > MESSAGE_SIZE) {
					return buffer.substring(0, MESSAGE_SIZE);
				}
				for (int i = buffer.length(); i < MESSAGE_SIZE; i++) {
					buffer.append(' ');
				}

				return buffer.toString();
			}
		};
		producer.start();
	
		
		// Give the forwarding clients a chance to get going and fill the down
		// stream broker queues..
		Thread.sleep(BROKER_COUNT*200);
		
        for (int i = 0; i < SAMPLES; i++) {

            long start = System.currentTimeMillis();
            producedMessages.set(0);
            receivedMessages.set(0);
            for (int j = 0; j < forwardingClients.length; j++) {
    			forwardingClients[j].forwardCounter.set(0);
    		}

            Thread.sleep(SAMPLE_DURATION);

            long end = System.currentTimeMillis();
            long r = receivedMessages.get();
            long p = producedMessages.get();

            LOG.info("published: " + p + " msgs at " + (p * 1000f / (end - start)) + " msgs/sec, " + "consumed: " + r + " msgs at " + (r * 1000f / (end - start)) + " msgs/sec");
            
            StringBuffer fwdingmsg = new StringBuffer(500);
            fwdingmsg.append("  forwarding counters: ");
            for (int j = 0; j < forwardingClients.length; j++) {
            	if( j!= 0 ) {
            		fwdingmsg.append(", ");
            	}
                fwdingmsg.append(forwardingClients[j].forwardCounter.get());
    		}
            LOG.info(fwdingmsg);

            // The test is just checking to make sure thaat the producer and consumer does not hang
            // due to the network hops take to route the message form the producer to the consumer.
            assertTrue("Recieved some messages since last sample", r>0);
            assertTrue("Produced some messages since last sample", p>0);
            
        }
        LOG.info("Sample done.");
        done.set(true);
        // Wait for the producer to finish.
        producer.join(1000*5);
        toConnection.close();
        fromConnection.close();
        
	}


}
