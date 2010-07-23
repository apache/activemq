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
package org.apache.activemq.transport.failover;

import static org.junit.Assert.assertTrue;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQMessageTransformation;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

// see https://issues.apache.org/activemq/browse/AMQ-2573
public class FailoverConsumerUnconsumedTest {
	
    private static final Log LOG = LogFactory.getLog(FailoverConsumerUnconsumedTest.class);
	private static final String QUEUE_NAME = "FailoverWithUnconsumed";
	private String url = "tcp://localhost:61616";
	final int prefetch = 10;
	BrokerService broker;
	
	public void startCleanBroker() throws Exception {
	    startBroker(true);
	}
	
	@After
	public void stopBroker() throws Exception {
	    if (broker != null) {
	        broker.stop();
	    }
	}
	
	public void startBroker(boolean deleteAllMessagesOnStartup) throws Exception {
	    broker = createBroker(deleteAllMessagesOnStartup);
        broker.start();
	}

	public BrokerService createBroker(boolean deleteAllMessagesOnStartup) throws Exception {   
	    broker = new BrokerService();
	    broker.addConnector(url);
	    broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);
	    return broker;
	}

	@Test
	public void testFailoverConsumerDups() throws Exception {
	    doTestFailoverConsumerDups(true);
	}
	 
	@Test
    public void testFailoverConsumerDupsNoAdvisoryWatch() throws Exception {
        doTestFailoverConsumerDups(false);
    }
	
	public void doTestFailoverConsumerDups(final boolean watchTopicAdvisories) throws Exception {
	    
	    final int maxConsumers = 4;
        broker = createBroker(true);
            
        broker.setPlugins(new BrokerPlugin[] {
                new BrokerPluginSupport() {
                    int consumerCount;

                    // broker is killed on x create consumer
                    @Override
                    public Subscription addConsumer(ConnectionContext context,
                            final ConsumerInfo info) throws Exception {
                         if (++consumerCount == maxConsumers + (watchTopicAdvisories ? 1:0)) {
                             context.setDontSendReponse(true);
                             Executors.newSingleThreadExecutor().execute(new Runnable() {   
                                 public void run() {
                                     LOG.info("Stopping broker on consumer: " + info.getConsumerId());
                                     try {
                                         broker.stop();
                                     } catch (Exception e) {
                                         e.printStackTrace();
                                     }
                                 }
                             });
                         }
                        return super.addConsumer(context, info);
                    }
                }
        });
        broker.start();
        
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        cf.setWatchTopicAdvisories(watchTopicAdvisories);
        
        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();
        
        final Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = consumerSession.createQueue(QUEUE_NAME + "?jms.consumer.prefetch=" + prefetch);

        final Vector<TestConsumer> testConsumers = new Vector<TestConsumer>();
        for (int i=0; i<maxConsumers -1; i++) {
            testConsumers.add(new TestConsumer(consumerSession, destination, connection));
        }
        
        produceMessage(consumerSession, destination, maxConsumers * prefetch);
               
        assertTrue("add messages are dispatched", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalUnconsumed = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long unconsumed = testConsumer.unconsumedSize();
                    LOG.info(testConsumer.getConsumerId() + " unconsumed: " + unconsumed);
                    totalUnconsumed += unconsumed;
                }   
                return totalUnconsumed == (maxConsumers-1) * prefetch;
            }
        }));
        
        final CountDownLatch commitDoneLatch = new CountDownLatch(1);
        
        Executors.newSingleThreadExecutor().execute(new Runnable() {   
            public void run() {
                try {
                    LOG.info("add last consumer...");
                    testConsumers.add(new TestConsumer(consumerSession, destination, connection));
                    commitDoneLatch.countDown();
                    LOG.info("done add last consumer");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
               
        // will be stopped by the plugin
        broker.waitUntilStopped();

        // verify interrupt
        assertTrue("add messages dispatched and unconsumed are cleaned up", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalUnconsumed = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long unconsumed = testConsumer.unconsumedSize();
                    LOG.info(testConsumer.getConsumerId() + " unconsumed: " + unconsumed);
                    totalUnconsumed += unconsumed;
                }
                return totalUnconsumed == 0;
            }
        }));

        broker = createBroker(false);
        broker.start();

        assertTrue("consumer added through failover", commitDoneLatch.await(30, TimeUnit.SECONDS));
        
        // each should again get prefetch messages - all unconsumed deliveries should be rolledback
        assertTrue("after start all messages are re dispatched", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalUnconsumed = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long unconsumed = testConsumer.unconsumedSize();
                    LOG.info(testConsumer.getConsumerId() + " after restart: unconsumed: " + unconsumed);
                    totalUnconsumed += unconsumed;
                }   
                return totalUnconsumed == (maxConsumers) * prefetch;
            }
        }));
        
        connection.close();
    }
        
    private void produceMessage(final Session producerSession, Queue destination, long count)
        throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);
        for (int i=0; i<count; i++) {
            TextMessage message = producerSession.createTextMessage("Test message " + i);
            producer.send(message);
        }
        producer.close();
    }
    
    // allow access to unconsumedMessages
    class TestConsumer extends ActiveMQMessageConsumer {
        
        TestConsumer(Session consumerSession, Destination destination, ActiveMQConnection connection) throws Exception {
            super((ActiveMQSession) consumerSession, 
                new ConsumerId(new SessionId(connection.getConnectionInfo().getConnectionId(),1), nextGen()), 
                ActiveMQMessageTransformation.transformDestination(destination), null, "",
                prefetch, -1, false, false, true, null);
        }
    
        public int unconsumedSize() {
            return unconsumedMessages.size();
        }
    }
    
    static long idGen = 100;
    private static long nextGen() {
        idGen -=5;
        return idGen;
    }
}
