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
package org.apache.activemq.broker.store;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;
import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.activemq.store.amq.AMQTransactionStore;
import org.apache.activemq.store.amq.AMQTx;

/**
 * Once the wire format is completed we can test against real persistence storage.
 * 
 * @version $Revision$
 */
public class TransactionStoreTest extends TestCase {

    protected static final int MAX_TX = 2500;
    protected static final int MAX_THREADS = 200;
    
    class UnderTest extends AMQTransactionStore {
        public UnderTest() {
            super(null);
        }
        public Map<TransactionId, AMQTx>  getInFlight() {
         return inflightTransactions;   
        }
    };
    
    UnderTest underTest = new UnderTest();
    
  public void testConcurrentGetTx() throws Exception {
      final ConnectionId connectionId = new ConnectionId("1:1");
      
      Runnable getTx = new Runnable() {
          
        public void run() {
            for (int i=0; i<MAX_TX;i++) {
                TransactionId txid = new LocalTransactionId(connectionId, i);
                underTest.getTx(txid, null);
            }
        }
      };
      
      ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);
      for (int i=0;i < MAX_THREADS; i++) {
          executor.execute(getTx);
      }
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
      
      assertEquals("has just the right amount of transactions", MAX_TX, underTest.getInFlight().size());
  }
}
