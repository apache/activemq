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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.amq.AMQTransactionStore;
import org.apache.activemq.store.amq.AMQTx;

public class TransactionStoreTest extends TestCase {

    protected static final int MAX_TX = 2500;
    protected static final int MAX_THREADS = 200;
    
    class BeingTested extends AMQTransactionStore {
        public BeingTested() {
            super(null);
        }
        public Map<TransactionId, AMQTx>  getInFlight() {
            return inflightTransactions;   
        }
    };
    
    BeingTested underTest = new BeingTested();
    
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
