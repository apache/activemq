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
package org.apache.activemq.broker.region.cursors;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StorePendingQueueMessageStoragePolicy;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;

/**
 * Modified CursorSupport Unit test to reproduce the negative queue issue.
 * 
 * Keys to reproducing:
 * 1) Consecutive queues with listener on first sending to second queue
 * 2) Push each queue to the memory limit
 *      This seems to help reproduce the issue more consistently, but
 *      we have seen times in our production environment where the
 *      negative queue can occur without. Our memory limits are
 *      very high in production and it still happens in varying 
 *      frequency.
 * 3) Prefetch
 *      Lowering the prefetch down to 10 and below seems to help 
 *      reduce occurrences. 
 * 4) # of consumers per queue
 *      The issue occurs less with fewer consumers
 * 
 * Things that do not affect reproduction:
 * 1) Spring - we use spring in our production applications, but this test case works
 *      with or without it.
 * 2) transacted
 * 
 */
public class NegativeQueueTest extends TestCase {

    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd,hh:mm:ss:SSS");
    
    private static final String QUEUE_1_NAME = "conn.test.queue.1";
    private static final String QUEUE_2_NAME = "conn.test.queue.2";
    
    private static final long QUEUE_MEMORY_LIMIT = 2097152;
    private static final long MEMORY_USAGE = 400000000;
    private static final long TEMP_USAGE = 200000000;
    private static final long STORE_USAGE = 1000000000;
    private static final int MESSAGE_COUNT = 2000;  
    
    protected static final boolean TRANSACTED = true;
    protected static final boolean DEBUG = false;
    protected static int NUM_CONSUMERS = 20;    
    protected static int PREFETCH_SIZE = 1000;  
    
    protected BrokerService broker;
    protected String bindAddress = "tcp://localhost:60706";
    
    public void testWithDefaultPrefetch() throws Exception{
        PREFETCH_SIZE = 1000;
        NUM_CONSUMERS = 20;
        blastAndConsume();
    }
    
    public void testWithDefaultPrefetchFiveConsumers() throws Exception{
        PREFETCH_SIZE = 1000;
        NUM_CONSUMERS = 5;
        blastAndConsume();
    }
    
    public void testWithDefaultPrefetchTwoConsumers() throws Exception{
        PREFETCH_SIZE = 1000;
        NUM_CONSUMERS = 2;
        blastAndConsume();
    }
    
    public void testWithDefaultPrefetchOneConsumer() throws Exception{
        PREFETCH_SIZE = 1000;
        NUM_CONSUMERS = 1;
        blastAndConsume();
    }
    
    public void testWithMediumPrefetch() throws Exception{
        PREFETCH_SIZE = 50;
        NUM_CONSUMERS = 20;
        blastAndConsume();
    }   
    
    public void testWithSmallPrefetch() throws Exception{
        PREFETCH_SIZE = 10;
        NUM_CONSUMERS = 20;
        blastAndConsume();
    }
    
    public void testWithNoPrefetch() throws Exception{
        PREFETCH_SIZE = 1;
        blastAndConsume();
    }
    
    public void blastAndConsume() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        
        //get proxy queues for statistics lookups
        Connection proxyConnection = factory.createConnection();
        proxyConnection.start();
        Session proxySession = proxyConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final QueueViewMBean proxyQueue1 = getProxyToQueueViewMBean((Queue)proxySession.createQueue(QUEUE_1_NAME));
        final QueueViewMBean proxyQueue2 = getProxyToQueueViewMBean((Queue)proxySession.createQueue(QUEUE_2_NAME));
        
        // LOAD THE QUEUE
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        Session session = producerConnection.createSession(TRANSACTED, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue(QUEUE_1_NAME);
        MessageProducer producer = session.createProducer(queue);
        List<TextMessage> senderList = new ArrayList<TextMessage>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            TextMessage msg = session.createTextMessage(formatter.format(new Date()));
            senderList.add(msg);
            producer.send(msg);
            if(TRANSACTED) session.commit();
            if(DEBUG && i%100 == 0){
                int index = (i/100)+1;
                System.out.print(index-((index/10)*10));
            }
        }
        
        //get access to the Queue info
        if(DEBUG){
            System.out.println("");
            System.out.println("Queue1 Size = "+proxyQueue1.getQueueSize());
            System.out.println("Queue1 Memory % Used = "+proxyQueue1.getMemoryPercentUsage());
            System.out.println("Queue1 Memory Available = "+proxyQueue1.getMemoryLimit());
        }
        
        // FLUSH THE QUEUE
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);        
        Connection[] consumerConnections1 = new Connection[NUM_CONSUMERS];
        List<Message> consumerList1 = new ArrayList<Message>();
        Connection[] consumerConnections2 = new Connection[NUM_CONSUMERS];
        Connection[] producerConnections2 = new Connection[NUM_CONSUMERS];
        List<Message> consumerList2 = new ArrayList<Message>();
        
        for(int ix=0; ix<NUM_CONSUMERS; ix++){
            producerConnections2[ix] = factory.createConnection();
            producerConnections2[ix].start();
            consumerConnections1[ix] = getConsumerConnection(factory);
            Session consumerSession = consumerConnections1[ix].createSession(TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(session.createQueue(QUEUE_1_NAME));
            consumer.setMessageListener(new SessionAwareMessageListener(producerConnections2[ix], consumerSession, QUEUE_2_NAME, latch1, consumerList1));
        }
        
        latch1.await(300000, TimeUnit.MILLISECONDS);
        if(DEBUG){
            System.out.println("");
            System.out.println("Queue2 Size = "+proxyQueue2.getQueueSize());
            System.out.println("Queue2 Memory % Used = "+proxyQueue2.getMemoryPercentUsage());
            System.out.println("Queue2 Memory Available = "+proxyQueue2.getMemoryLimit());
        }
        
        for(int ix=0; ix<NUM_CONSUMERS; ix++){
            consumerConnections2[ix] = getConsumerConnection(factory);
            Session consumerSession = consumerConnections2[ix].createSession(TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(session.createQueue(QUEUE_2_NAME));
            consumer.setMessageListener(new SessionAwareMessageListener(consumerSession, latch2, consumerList2));
        }
        
        latch2.await(300000, TimeUnit.MILLISECONDS);
        producerConnection.close();
        for(int ix=0; ix<NUM_CONSUMERS; ix++){
            consumerConnections1[ix].close();
            consumerConnections2[ix].close();
            producerConnections2[ix].close();
        }
        
        //let the consumer statistics on queue2 have time to update
        Thread.sleep(500);
        
        if(DEBUG){
            System.out.println("");
            System.out.println("Queue1 Size = "+proxyQueue1.getQueueSize());
            System.out.println("Queue1 Memory % Used = "+proxyQueue1.getMemoryPercentUsage());
            System.out.println("Queue2 Size = "+proxyQueue2.getQueueSize());
            System.out.println("Queue2 Memory % Used = "+proxyQueue2.getMemoryPercentUsage());
        }
        
        assertEquals("Queue1 has gone negative,",0, proxyQueue1.getQueueSize());
        assertEquals("Queue2 has gone negative,",0, proxyQueue2.getQueueSize());
        proxyConnection.close();
        
    }

    private QueueViewMBean getProxyToQueueViewMBean(Queue queue)
        throws MalformedObjectNameException, JMSException {
        
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq" + ":Type=Queue,Destination=" + 
            queue.getQueueName() + ",BrokerName=localhost");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext().newProxyInstance(queueViewMBeanName,
                QueueViewMBean.class, true);
     
        return proxy;
    }
    
   protected Connection getConsumerConnection(ConnectionFactory fac) throws JMSException {
        Connection connection = fac.createConnection();
        connection.start();
        return connection;
    }

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(bindAddress);
        Properties props = new Properties();
        props.setProperty("prefetchPolicy.durableTopicPrefetch", "" + PREFETCH_SIZE);
        props.setProperty("prefetchPolicy.optimizeDurableTopicPrefetch", "" + PREFETCH_SIZE);
        props.setProperty("prefetchPolicy.queuePrefetch", "" + PREFETCH_SIZE);
        cf.setProperties(props);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }
    
    protected void configureBroker(BrokerService answer) throws Exception {
        PolicyEntry policy = new PolicyEntry();
        policy.setMemoryLimit(QUEUE_MEMORY_LIMIT); 
        policy.setPendingQueuePolicy(new StorePendingQueueMessageStoragePolicy());
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        answer.setDestinationPolicy(pMap);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(bindAddress);

        MemoryUsage memoryUsage = new MemoryUsage();
        memoryUsage.setLimit(MEMORY_USAGE); 
        memoryUsage.setPercentUsageMinDelta(20);

        TempUsage tempUsage = new TempUsage();
        tempUsage.setLimit(TEMP_USAGE); 

        StoreUsage storeUsage = new StoreUsage();
        storeUsage.setLimit(STORE_USAGE); 

        SystemUsage systemUsage = new SystemUsage();
        systemUsage.setMemoryUsage(memoryUsage);
        systemUsage.setTempUsage(tempUsage);
        systemUsage.setStoreUsage(storeUsage);
        answer.setSystemUsage(systemUsage);
    }
    
    /**
     * Message listener that is given the Session for transacted consumers
     */
    class SessionAwareMessageListener implements MessageListener{
        private List<Message> consumerList;
        private CountDownLatch latch;
        private Session consumerSession;
        private Session producerSession;
        private MessageProducer producer;
        
        public SessionAwareMessageListener(Session consumerSession, CountDownLatch latch, List<Message> consumerList){
            this(null, consumerSession, null, latch, consumerList);
        }
        
        public SessionAwareMessageListener(Connection producerConnection, Session consumerSession, String outQueueName, 
                CountDownLatch latch, List<Message> consumerList){
            this.consumerList = consumerList;
            this.latch = latch;
            this.consumerSession = consumerSession;
            
            if(producerConnection != null){
                try {
                    producerSession = producerConnection.createSession(TRANSACTED, Session.AUTO_ACKNOWLEDGE);
                    Destination queue = producerSession.createQueue(outQueueName);
                    producer = producerSession.createProducer(queue);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
        
        public void onMessage(Message msg) {
            try {
                if(producer == null){
                    // sleep to act as a slow consumer
                    // which will force a mix of direct and polled dispatching
                    // using the cursor on the broker
                    Thread.sleep(50);
                }else{
                    producer.send(msg);
                    if(TRANSACTED) producerSession.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            synchronized(consumerList){
                consumerList.add(msg);
                if(DEBUG && consumerList.size()%100 == 0) {
                    int index = consumerList.size()/100;
                    System.out.print(index-((index/10)*10));
                }
                if (consumerList.size() == MESSAGE_COUNT) {
                    latch.countDown();
                }
            }
            if(TRANSACTED){
                try {
                    consumerSession.commit();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }    
}