/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.bugs;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class AMQ6815Test {
   static final Logger LOG = LoggerFactory.getLogger(AMQ6815Test.class);
   private final static int MEM_LIMIT = 1024*1024; // 1 MB
   private final static byte[] payload = new byte[1024];

      protected BrokerService brokerService;
      protected Connection connection;
      protected Session session;
      protected Queue amqDestination;

      @Before
      public void setUp() throws Exception {
         brokerService = new BrokerService();
         PolicyEntry policy = new PolicyEntry();
         policy.setMemoryLimit(MEM_LIMIT);
         PolicyMap pMap = new PolicyMap();
         pMap.setDefaultEntry(policy);
         brokerService.setDestinationPolicy(pMap);

         brokerService.start();
         connection = new ActiveMQConnectionFactory("vm://localhost").createConnection();
         connection.start();
         session = connection.createSession(false, ActiveMQSession.AUTO_ACKNOWLEDGE);
         amqDestination = session.createQueue("QQ");
      }

      @After
      public void tearDown() throws Exception {
         try {
            connection.close();
         } catch (Exception e) {
            //swallow any error so broker can still be stopped
         }
         brokerService.stop();
      }

      @Test(timeout = 240000)
      public void testHeapUsage() throws Exception {
         Runtime.getRuntime().gc();
         final long initUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
         LOG.info("Initial Mem in use: " + initUsedMemory/1024  + "K");

         sendMessages(5000); // 5k of 1k messages = 5MB and limit is 1MB so some will be paged to disk

         Runtime.getRuntime().gc();
         long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() - initUsedMemory;
         LOG.info("Mem in use: " + usedMem/1024  + "K");

          // 2 is a big generous factor because we don't create this many additional objects per message
         assertTrue("Used Mem reasonable " + usedMem, usedMem < 5 * MEM_LIMIT);
      }

      protected void sendMessages(int count) throws JMSException {
         MessageProducer producer = session.createProducer(amqDestination);
         for (int i = 0; i < count; i++) {
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(payload);
            producer.send(bytesMessage);
         }
         producer.close();
      }

}
