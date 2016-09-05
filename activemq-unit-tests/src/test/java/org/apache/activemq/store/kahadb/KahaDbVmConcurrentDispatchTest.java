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
package org.apache.activemq.store.kahadb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.AbstractVmConcurrentDispatchTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KahaDbVmConcurrentDispatchTest extends AbstractVmConcurrentDispatchTest {

    private final boolean concurrentDispatch;
    private static boolean[] concurrentDispatchVals = booleanVals;

      @Parameters(name="Type:{0}; ReduceMemoryFootPrint:{1}; ConcurrentDispatch:{2}; UseTopic:{3}")
      public static Collection<Object[]> data() {
          List<Object[]> values = new ArrayList<>();

          for (MessageType mt : MessageType.values()) {
              for (boolean rmfVal : reduceMemoryFootPrintVals) {
                  for (boolean cdVal : concurrentDispatchVals) {
                      for (boolean tpVal : useTopicVals) {
                          values.add(new Object[] {mt, rmfVal, cdVal, tpVal});
                      }
                  }
              }
          }

          return values;
      }

    /**
     * @param messageType
     * @param reduceMemoryFootPrint
     * @param concurrentDispatch
     */
    public KahaDbVmConcurrentDispatchTest(MessageType messageType, boolean reduceMemoryFootPrint,
            boolean concurrentDispatch, boolean useTopic) {
        super(messageType, reduceMemoryFootPrint, useTopic);
        this.concurrentDispatch = concurrentDispatch;
    }

    @Override
    protected void configurePersistenceAdapter(BrokerService broker) throws IOException {
        KahaDBPersistenceAdapter ad = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        if (useTopic) {
            ad.setConcurrentStoreAndDispatchTopics(concurrentDispatch);
        } else {
            ad.setConcurrentStoreAndDispatchQueues(concurrentDispatch);
        }
    }

}
