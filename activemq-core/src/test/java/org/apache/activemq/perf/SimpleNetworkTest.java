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
import javax.jms.Destination;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class SimpleNetworkTest extends SimpleTopicTest {

    private static final Log LOG = LogFactory.getLog(SimpleNetworkTest.class);
    protected String consumerBindAddress = "tcp://localhost:61616";
    protected String producerBindAddress = "tcp://localhost:61617";
    protected static final String CONSUMER_BROKER_NAME = "Consumer";
    protected static final String PRODUCER_BROKER_NAME = "Producer";
    protected BrokerService consumerBroker;
    protected BrokerService producerBroker;
    protected ActiveMQConnectionFactory consumerFactory;
    protected ActiveMQConnectionFactory producerFactory;
    
    
    protected void setUp() throws Exception {
        if (consumerBroker == null) {
           consumerBroker = createConsumerBroker(consumerBindAddress);
        }
        if (producerBroker == null) {
            producerBroker = createProducerBroker(producerBindAddress);
        }
        consumerFactory = createConnectionFactory(consumerBindAddress);
        consumerFactory.setDispatchAsync(true);
        ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
        policy.setQueuePrefetch(100);
        consumerFactory.setPrefetchPolicy(policy);
        producerFactory = createConnectionFactory(producerBindAddress);
        Connection con = consumerFactory.createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        producers = new PerfProducer[numberofProducers*numberOfDestinations];
        consumers = new PerfConsumer[numberOfConsumers*numberOfDestinations];
        
        for (int k =0; k < numberOfDestinations;k++) {
            Destination destination = createDestination(session, destinationName+":"+k);
            LOG.info("Testing against destination: " + destination);
            for (int i = 0; i < numberOfConsumers; i++) {
                consumers[i] = createConsumer(consumerFactory, destination, i);
                consumers[i].start();
            }
            for (int i = 0; i < numberofProducers; i++) {
                array = new byte[playloadSize];
                for (int j = i; j < array.length; j++) {
                    array[j] = (byte)j;
                }
                producers[i] = createProducer(producerFactory, destination, i, array);
                producers[i].start();
               
            }
        }
        con.close();
    }

    protected void tearDown() throws Exception {
        for (int i = 0; i < numberOfConsumers; i++) {
            consumers[i].shutDown();
        }
        for (int i = 0; i < numberofProducers; i++) {
            producers[i].shutDown();
        }
        
        if (producerBroker != null) {
            producerBroker.stop();
            producerBroker = null;
        }
        if (consumerBroker != null) {
            consumerBroker.stop();
            consumerBroker = null;
        }
    }
    
    protected BrokerService createConsumerBroker(String uri) throws Exception {
        BrokerService answer = new BrokerService();
        configureConsumerBroker(answer,uri);
        answer.start();
        return answer;
    }
    
    protected void configureConsumerBroker(BrokerService answer,String uri) throws Exception {
        configureBroker(answer);
        answer.setPersistent(false);
        answer.setBrokerName(CONSUMER_BROKER_NAME);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(uri);
        answer.setUseShutdownHook(false);
    }
    
    protected BrokerService createProducerBroker(String uri) throws Exception {
        BrokerService answer = new BrokerService();
        configureProducerBroker(answer,uri);
        answer.start();
        return answer;
    }
    
    protected void configureProducerBroker(BrokerService answer,String uri) throws Exception {
        configureBroker(answer);
        answer.setBrokerName(PRODUCER_BROKER_NAME);
        answer.setMonitorConnectionSplits(false);
        //answer.setSplitSystemUsageForProducersConsumers(true);
        answer.setPersistent(false);
        answer.setDeleteAllMessagesOnStartup(true);
        NetworkConnector connector = answer.addNetworkConnector("static://"+consumerBindAddress);
        //connector.setNetworkTTL(3);
        //connector.setDynamicOnly(true);
        connector.setDuplex(true);
        answer.addConnector(uri);
        answer.setUseShutdownHook(false);
    }
    
    protected void configureBroker(BrokerService service) throws Exception{
        
    }


}
