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
package org.apache.activemq.advisory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;

/**
 * 
 * @version $Revision: 397249 $
 */
public class TempDestDeleteTest extends EmbeddedBrokerTestSupport implements ConsumerListener {

    protected int consumerCounter;
    protected ConsumerEventSource topicConsumerEventSource;
    private ConsumerEventSource queueConsumerEventSource;

    protected BlockingQueue eventQueue = new ArrayBlockingQueue(1000);
    private Connection connection;
    private Session session;
    private ActiveMQTempTopic tempTopic;
    private ActiveMQTempQueue tempQueue;

    public void testDeleteTempTopicDeletesAvisoryTopics() throws Exception {
        topicConsumerEventSource.start();

        MessageConsumer consumer = createConsumer(tempTopic);
        assertConsumerEvent(1, true);

        Topic advisoryTopic = AdvisorySupport.getConsumerAdvisoryTopic(tempTopic);
        assertTrue( destinationExists(advisoryTopic) );
        
        consumer.close();
        
        // Once we delete the topic, the advisory topic for the destination should also be deleted.
        tempTopic.delete();
        
        assertFalse( destinationExists(advisoryTopic) );
    }

    public void testDeleteTempQueueDeletesAvisoryTopics() throws Exception {
        queueConsumerEventSource.start();

        MessageConsumer consumer = createConsumer(tempQueue);
        assertConsumerEvent(1, true);

        Topic advisoryTopic = AdvisorySupport.getConsumerAdvisoryTopic(tempQueue);
        assertTrue( destinationExists(advisoryTopic) );
        
        consumer.close();
        
        // Once we delete the queue, the advisory topic for the destination should also be deleted.
        tempQueue.delete();
        
        assertFalse( destinationExists(advisoryTopic) );
    }

    private boolean destinationExists(Destination dest) throws Exception {
        RegionBroker rb = (RegionBroker) broker.getBroker().getAdaptor(RegionBroker.class);
        return rb.getTopicRegion().getDestinationMap().containsKey(dest)
                || rb.getQueueRegion().getDestinationMap().containsKey(dest)
                || rb.getTempTopicRegion().getDestinationMap().containsKey(dest)
                || rb.getTempQueueRegion().getDestinationMap().containsKey(dest);
    }

    public void onConsumerEvent(ConsumerEvent event) {
        eventQueue.add(event);
    }

    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
        connection.start();
        
        session = connection.createSession(false, 0);
        
        tempTopic = (ActiveMQTempTopic) session.createTemporaryTopic();
        topicConsumerEventSource = new ConsumerEventSource(connection, tempTopic);
        topicConsumerEventSource.setConsumerListener(this);
    
        tempQueue = (ActiveMQTempQueue) session.createTemporaryQueue();
        queueConsumerEventSource = new ConsumerEventSource(connection, tempQueue);
        queueConsumerEventSource.setConsumerListener(this);
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected void assertConsumerEvent(int count, boolean started) throws InterruptedException {
        ConsumerEvent event = waitForConsumerEvent();
        assertEquals("Consumer count", count, event.getConsumerCount());
        assertEquals("started", started, event.isStarted());
    }

    protected MessageConsumer createConsumer(Destination dest) throws JMSException {
        final String consumerText = "Consumer: " + (++consumerCounter);
        LOG.info("Creating consumer: " + consumerText + " on destination: " + dest);
        
        MessageConsumer consumer = session.createConsumer(dest);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                LOG.info("Received message by: " + consumerText + " message: " + message);
            }
        });
        return consumer;
    }

    protected ConsumerEvent waitForConsumerEvent() throws InterruptedException {
        ConsumerEvent answer = (ConsumerEvent) eventQueue.poll(1000, TimeUnit.MILLISECONDS);
        assertTrue("Should have received a consumer event!", answer != null);
        return answer;
    }

}
