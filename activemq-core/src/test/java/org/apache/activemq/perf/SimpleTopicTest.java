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
package org.apache.activemq.perf;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.3 $
 */
public class SimpleTopicTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(SimpleTopicTest.class);
    
    protected BrokerService broker;
    protected String clientURI="tcp://localhost:61616?wireFormat.cacheEnabled=true&wireFormat.tightEncodingEnabled=true&jms.useAsyncSend=false&wireFormat.maxInactivityDuration=0";
    //protected String clientURI="tcp://localhost:61616";
    protected String bindAddress="tcp://localhost:61616?wireFormat.maxInactivityDuration=0";
    //protected String bindAddress = "tcp://localhost:61616";
    //protected String bindAddress="vm://localhost?marshal=true";
    //protected String bindAddress="vm://localhost";
    protected PerfProducer[] producers;
    protected PerfConsumer[] consumers;
    protected String destinationName = getClass().getName();
    protected int sampleCount = 20;
    protected long sampleInternal = 10000;
    protected int numberOfDestinations=1;
    protected int numberOfConsumers = 1;
    protected int numberofProducers = 1;
    protected int totalNumberOfProducers;
    protected int totalNumberOfConsumers;
    protected int playloadSize = 12;
    protected byte[] array;
    protected ConnectionFactory factory;
    
     /**
     * Sets up a test where the producer and consumer have their own connection.
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        //if (broker == null) {
        //    broker = createBroker(bindAddress);
        //}
        factory = createConnectionFactory(clientURI);
        Connection con = factory.createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
       
        
        LOG.info("Running " + numberofProducers + " producer(s) and " + numberOfConsumers + " consumer(s) per " + numberOfDestinations + " Destination(s)");
       
        totalNumberOfConsumers=numberOfConsumers*numberOfDestinations;
        totalNumberOfProducers=numberofProducers*numberOfDestinations;
        producers = new PerfProducer[totalNumberOfProducers];
        consumers = new PerfConsumer[totalNumberOfConsumers];
        int consumerCount = 0;
        int producerCount = 0;
        for (int k =0; k < numberOfDestinations;k++) {
            Destination destination = createDestination(session, destinationName+":"+k);
            LOG.info("Testing against destination: " + destination);
            for (int i = 0; i < numberOfConsumers; i++) {
                consumers[consumerCount] = createConsumer(factory, destination, consumerCount);
                consumerCount++;
            }
            for (int i = 0; i < numberofProducers; i++) {
                array = new byte[playloadSize];
                for (int j = i; j < array.length; j++) {
                    array[j] = (byte)j;
                }
                producers[producerCount] = createProducer(factory, destination, i, array);
                producerCount++;
            }
        }
        con.close();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        for (int i = 0; i < numberOfConsumers; i++) {
            consumers[i].shutDown();
        }
        for (int i = 0; i < numberofProducers; i++) {
            producers[i].shutDown();
        }
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

    protected Destination createDestination(Session s, String destinationName) throws JMSException {
        return s.createTopic(destinationName);
    }

    /**
     * Factory method to create a new broker
     * 
     * @throws Exception
     */
    protected BrokerService createBroker(String uri) throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer,uri);
        answer.start();
        return answer;
    }

    protected PerfProducer createProducer(ConnectionFactory fac, Destination dest, int number, byte[] payload) throws JMSException {
        return new PerfProducer(fac, dest, payload);
    }

    protected PerfConsumer createConsumer(ConnectionFactory fac, Destination dest, int number) throws JMSException {
        return new PerfConsumer(fac, dest);
    }
    
    
    protected void configureBroker(BrokerService answer,String uri) throws Exception {
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(uri);
        answer.setUseShutdownHook(false);
    }

    protected ActiveMQConnectionFactory createConnectionFactory(String uri) throws Exception {
        return new ActiveMQConnectionFactory(uri);
    }

    public void testPerformance() throws JMSException, InterruptedException {
        for (int i = 0; i < totalNumberOfConsumers; i++) {
            consumers[i].start();
        }
        for (int i = 0; i < totalNumberOfProducers; i++) {
            producers[i].start();
        }
        LOG.info("Sampling performance " + sampleCount + " times at a " + sampleInternal + " ms interval.");
        for (int i = 0; i < sampleCount; i++) {
            Thread.sleep(sampleInternal);
            dumpProducerRate();
            dumpConsumerRate();
        }
        for (int i = 0; i < totalNumberOfProducers; i++) {
            producers[i].stop();
        }
        for (int i = 0; i < totalNumberOfConsumers; i++) {
            consumers[i].stop();
        }
    }

    protected void dumpProducerRate() {
        int totalRate = 0;
        int totalCount = 0;
        String producerString="Producers:";
        for (int i = 0; i < producers.length; i++) {
            PerfRate rate = producers[i].getRate().cloneAndReset();
            totalRate += rate.getRate();
            totalCount += rate.getTotalCount();
            producerString+="["+i+":"+rate.getRate() + ","+rate.getTotalCount()+"];";
        }
        if (producers != null && producers.length > 0) {
            int avgRate = totalRate / producers.length;
            System.out.println("Avg producer rate = " + avgRate
                    + " msg/sec | Total rate = " + totalRate + ", sent = "
                    + totalCount);
           // System.out.println(producerString);
        }
    }

    protected void dumpConsumerRate() {
        int totalRate = 0;
        int totalCount = 0;
        String consumerString="Consumers:";
        for (int i = 0; i < consumers.length; i++) {
            PerfRate rate = consumers[i].getRate().cloneAndReset();
            totalRate += rate.getRate();
            totalCount += rate.getTotalCount();
            consumerString+="["+i+":"+rate.getRate() + ","+rate.getTotalCount()+"];";
        }
        if (consumers != null && consumers.length > 0) {
            int avgRate = totalRate / consumers.length;
            System.out.println("Avg consumer rate = " + avgRate + " msg/sec | Total rate = " + totalRate + ", received = " + totalCount);
            System.out.println(consumerString);
        }
    }
}
