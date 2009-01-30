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
package org.apache.activemq.usecases;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class MessageGroupDelayedTest extends JmsTestSupport {
  public static final Log log = LogFactory.getLog(MessageGroupDelayedTest.class);
  protected Connection connection;
  protected Session session;
  protected MessageProducer producer;
  protected Destination destination;
  
  public int consumersBeforeDispatchStarts;
  public int timeBeforeDispatchStarts;
  
  BrokerService broker;
  protected TransportConnector connector;
  
  protected HashMap<String, Integer> messageCount = new HashMap<String, Integer>();
  protected HashMap<String, Set<String>> messageGroups = new HashMap<String, Set<String>>();
  
  public static Test suite() {
      return suite(MessageGroupDelayedTest.class);
  }

  public static void main(String[] args) {
      junit.textui.TestRunner.run(suite());
  }

  public void setUp() throws Exception {
	broker = createBroker();  
	broker.start();
    ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory(connector.getConnectUri() + "?jms.prefetchPolicy.all=1");
    connection = connFactory.createConnection();
    session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
    destination = new ActiveMQQueue("test-queue2");
    producer = session.createProducer(destination);
    connection.start();
  }
  
  protected BrokerService createBroker() throws Exception {
      BrokerService service = new BrokerService();
      service.setPersistent(false);
      service.setUseJmx(false);

      // Setup a destination policy where it takes only 1 message at a time.
      PolicyMap policyMap = new PolicyMap();
      PolicyEntry policy = new PolicyEntry();
      log.info("testing with consumersBeforeDispatchStarts=" + consumersBeforeDispatchStarts + " and timeBeforeDispatchStarts=" + timeBeforeDispatchStarts);
      policy.setConsumersBeforeDispatchStarts(consumersBeforeDispatchStarts);
      policy.setTimeBeforeDispatchStarts(timeBeforeDispatchStarts);
      policyMap.setDefaultEntry(policy);
      service.setDestinationPolicy(policyMap);

      connector = service.addConnector("tcp://localhost:0");
      return service;
  }
  
  public void tearDown() throws Exception {
      producer.close();
      session.close();
      connection.close();
  }
  
  
  
  public void initCombosForTestDelayedDirectConnectionListener() {
	  addCombinationValues("consumersBeforeDispatchStarts", new Object[] {0, 3, 5});
	  addCombinationValues("timeBeforeDispatchStarts", new Object[] {0, 100});
  }
  
  public void testDelayedDirectConnectionListener() throws Exception {
	  
	for(int i = 0; i < 10; i++) {
      Message msga = session.createTextMessage("hello a");
      msga.setStringProperty("JMSXGroupID", "A");
      producer.send(msga);
      Message msgb = session.createTextMessage("hello b");
      msgb.setStringProperty("JMSXGroupID", "B");
      producer.send(msgb);
      Message msgc = session.createTextMessage("hello c");
      msgc.setStringProperty("JMSXGroupID", "C");
      producer.send(msgc);
    }
    log.info("30 messages sent to group A/B/C");
   
    int[] counters = {10, 10, 10};
    
    CountDownLatch startSignal = new CountDownLatch(1);
    CountDownLatch doneSignal = new CountDownLatch(1);

    messageCount.put("worker1", 0);
    messageGroups.put("worker1", new HashSet<String>());
    Worker worker1 = new Worker(connection, destination, "worker1", startSignal, doneSignal, counters, messageCount, messageGroups);
    messageCount.put("worker2", 0);
    messageGroups.put("worker2", new HashSet<String>());
    Worker worker2 = new Worker(connection, destination, "worker2", startSignal, doneSignal, counters, messageCount, messageGroups);
    messageCount.put("worker3", 0);
    messageGroups.put("worker3", new HashSet<String>());
    Worker worker3 = new Worker(connection, destination, "worker3", startSignal, doneSignal, counters, messageCount, messageGroups);


    new Thread(worker1).start();
    new Thread(worker2).start();
    new Thread(worker3).start();

    startSignal.countDown();
    doneSignal.await();
    
    // check results
    if (consumersBeforeDispatchStarts == 0 && timeBeforeDispatchStarts == 0) {
    	log.info("Ignoring results because both parameters are 0");
    	return;
    }
    
    for (String worker: messageCount.keySet()) {
    	log.info("worker " + worker + " received " + messageCount.get(worker) + " messages from groups " + messageGroups.get(worker));
    	assertEquals("worker " + worker + " received " + messageCount.get(worker) + " messages from groups " + messageGroups.get(worker)
    			, 10, messageCount.get(worker).intValue());
    	assertEquals("worker " + worker + " received " + messageCount.get(worker) + " messages from groups " + messageGroups.get(worker)
    			, 1, messageGroups.get(worker).size());
    }
    
  }

  private static final class Worker implements Runnable {
    private Connection connection = null;
    private Destination queueName = null;
    private String workerName = null;
    private CountDownLatch startSignal = null;
    private CountDownLatch doneSignal = null;
    private int[] counters = null;
    private HashMap<String, Integer> messageCount;
    private HashMap<String, Set<String>>messageGroups;
    
    
    private Worker(Connection connection, Destination queueName, String workerName, CountDownLatch startSignal, CountDownLatch doneSignal, int[] counters, HashMap<String, Integer> messageCount, HashMap<String, Set<String>>messageGroups) {
      this.connection = connection;
      this.queueName = queueName;
      this.workerName = workerName;
      this.startSignal = startSignal;
      this.doneSignal = doneSignal;
      this.counters = counters;
      this.messageCount = messageCount;
      this.messageGroups = messageGroups;
    }
    
    private void update(String group) {
        int msgCount = messageCount.get(workerName);
        messageCount.put(workerName, msgCount + 1);
        Set<String> groups = messageGroups.get(workerName);
        groups.add(group);
        messageGroups.put(workerName, groups);
    }
    
    public void run() {

      try {
        log.info(workerName);
        startSignal.await();
        Session sess = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = sess.createConsumer(queueName);

        while(true) {
          if(counters[0] == 0 && counters[1] == 0 && counters[2] == 0 ) {
            doneSignal.countDown();
            log.info(workerName + " done...");
            break;
          }
          
          Message msg = consumer.receive(500);
          if(msg == null)
            continue;

          String group = msg.getStringProperty("JMSXGroupID");
          boolean first = msg.getBooleanProperty("JMSXGroupFirstForConsumer");

          if("A".equals(group)){
        	--counters[0];
            update(group);
            Thread.sleep(500);
          }
          else if("B".equals(group)) {
        	--counters[1];
            update(group);
            Thread.sleep(100);
          }
          else if("C".equals(group)) {
        	--counters[2];
            update(group);
            Thread.sleep(10);
          }
          else {
            log.warn("unknown group");
          }
          if (counters[0] != 0 || counters[1] != 0 || counters[2] != 0 ) {
        	  msg.acknowledge();
          }
        }
        consumer.close();
        sess.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
