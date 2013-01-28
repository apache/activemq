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
package org.apache.activemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.TransactionRolledBackException;

import org.apache.activemq.transaction.Synchronization;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionContextTest {
    
    TransactionContext underTest;
    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
    ActiveMQConnection connection;
    
    
    @Before
    public void setup() throws Exception {
        connection = factory.createActiveMQConnection();
        underTest = new TransactionContext(connection);
    }
    
    @After
    public void tearDown() throws Exception {
        connection.close();
    }
    
    @Test
    public void testSyncBeforeEndCalledOnceOnRollback() throws Exception {
        final AtomicInteger beforeEndCountA = new AtomicInteger(0);
        final AtomicInteger beforeEndCountB = new AtomicInteger(0);
        final AtomicInteger rollbackCountA = new AtomicInteger(0);
        final AtomicInteger rollbackCountB = new AtomicInteger(0);
        underTest.addSynchronization(new Synchronization() {
            @Override
            public void beforeEnd() throws Exception {
                if (beforeEndCountA.getAndIncrement() == 0) {
                    throw new TransactionRolledBackException("force rollback");
                }
            }

            @Override
            public void afterCommit() throws Exception {
                fail("exepcted rollback exception");
            }

            @Override
            public void afterRollback() throws Exception {
                rollbackCountA.incrementAndGet();
            }
            
        });
        
        underTest.addSynchronization(new Synchronization() {
            @Override
            public void beforeEnd() throws Exception {
                beforeEndCountB.getAndIncrement();
            }
            
            @Override     
            public void afterCommit() throws Exception {
                fail("exepcted rollback exception");
            }

            @Override
            public void afterRollback() throws Exception {
                rollbackCountB.incrementAndGet();
            }

        });
        
        
        try {
            underTest.commit();
            fail("exepcted rollback exception");
        } catch (TransactionRolledBackException expected) {
        }
        
        assertEquals("beforeEnd A called once", 1, beforeEndCountA.get());
        assertEquals("beforeEnd B called once", 1, beforeEndCountA.get());
        assertEquals("rollbackCount B 0", 1, rollbackCountB.get());
        assertEquals("rollbackCount A B", rollbackCountA.get(), rollbackCountB.get());
    }
    
    @Test
    public void testSyncIndexCleared() throws Exception {
        final AtomicInteger beforeEndCountA = new AtomicInteger(0);
        final AtomicInteger rollbackCountA = new AtomicInteger(0);
        Synchronization sync = new Synchronization() {
            @Override
            public void beforeEnd() throws Exception {
                beforeEndCountA.getAndIncrement();
            }
            @Override
            public void afterCommit() throws Exception {
                fail("exepcted rollback exception");
            }
            @Override
            public void afterRollback() throws Exception {
                rollbackCountA.incrementAndGet();
            } 
        };
        
        underTest.begin();
        underTest.addSynchronization(sync);
        underTest.rollback();
        
        assertEquals("beforeEnd", 1, beforeEndCountA.get());
        assertEquals("rollback", 1, rollbackCountA.get());
     
        // do it again
        underTest.begin();
        underTest.addSynchronization(sync);
        underTest.rollback();
     
        assertEquals("beforeEnd", 2, beforeEndCountA.get());
        assertEquals("rollback", 2, rollbackCountA.get());
    }
}
