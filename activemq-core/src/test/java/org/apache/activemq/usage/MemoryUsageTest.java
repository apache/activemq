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

package org.apache.activemq.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MemoryUsageTest {

    MemoryUsage underTest;
      
    @Test
    public final void testPercentUsageNeedsNoThread() {    
        int activeThreadCount = Thread.activeCount();
        underTest.setLimit(10);
        underTest.start();
        underTest.increaseUsage(1);
        assertEquals("usage is correct", 10, underTest.getPercentUsage());
        assertEquals("no new thread created withough listener or callback",activeThreadCount, Thread.activeCount()); 
    }
    
    @Test
    public final void testAddUsageListenerStartsThread() throws Exception {       
        int activeThreadCount = Thread.activeCount();
        underTest = new MemoryUsage();
        underTest.setLimit(10);
        underTest.start();
        final CountDownLatch called = new CountDownLatch(1);
        final String[] listnerThreadNameHolder = new String[1];
        underTest.addUsageListener(new UsageListener() {
            public void onUsageChanged(Usage usage, int oldPercentUsage,
                    int newPercentUsage) {
                called.countDown();
                listnerThreadNameHolder[0] = Thread.currentThread().toString();
            }
        });
        underTest.increaseUsage(1);
        assertTrue("listner was called", called.await(30, TimeUnit.SECONDS));
        assertTrue("listner called from another thread", !Thread.currentThread().toString().equals(listnerThreadNameHolder[0]));
        assertEquals("usage is correct", 10, underTest.getPercentUsage());
        assertEquals("new thread created with listener", activeThreadCount + 1, Thread.activeCount());        
    }
    
    @Before
    public void setUp() throws Exception {
        underTest = new MemoryUsage();   
    }
    
    @After
    public void tearDown() {
        assertNotNull(underTest);
        underTest.stop();
    }
}
