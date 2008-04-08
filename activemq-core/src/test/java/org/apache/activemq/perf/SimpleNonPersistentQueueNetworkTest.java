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

import java.util.ArrayList;
import java.util.List;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

public class SimpleNonPersistentQueueNetworkTest extends SimpleNetworkTest {

	protected void setUp()throws Exception {
	    numberOfDestinations =20;
	    super.setUp();
	}
	protected PerfProducer createProducer(ConnectionFactory fac, Destination dest, int number, byte[] payload) throws JMSException {
        PerfProducer pp = new PerfProducer(fac, dest, payload);
       pp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
       // pp.setTimeToLive(1000);
        //pp.setSleep(1);
        return pp;
    }
	
	protected PerfConsumer createConsumer(ConnectionFactory fac, Destination dest, int number) throws JMSException {
        PerfConsumer consumer =  new PerfConsumer(fac, dest);
        boolean enableAudit = numberOfConsumers <= 1;
        System.out.println("Enable Audit = " + enableAudit);
        consumer.setEnableAudit(false);
        return consumer;
    }
	
	 public void testPerformance() throws JMSException, InterruptedException {
	     //Thread.sleep(5000);
	     super.testPerformance();
	 }
	
	 protected Destination createDestination(Session s, String destinationName) throws JMSException {
	        return s.createQueue(destinationName);
	 }
	 
	 protected void configureBroker(BrokerService answer) throws Exception {
	        answer.setPersistent(false);
	        answer.setMonitorConnectionSplits(true);
	        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
	        final PolicyEntry entry = new PolicyEntry();
	        entry.setQueue(">");
	        entry.setMemoryLimit(1024 * 1024 * 100); // Set to 1 MB
	        entry.setOptimizedDispatch(true);
	        entry.setProducerFlowControl(true);
	        entry.setMaxPageSize(10);
	        entry.setLazyDispatch(false);
	        policyEntries.add(entry);

	        
	        final PolicyMap policyMap = new PolicyMap();
	        policyMap.setPolicyEntries(policyEntries);
	        answer.setDestinationPolicy(policyMap);
	        super.configureBroker(answer);
	    }
}
