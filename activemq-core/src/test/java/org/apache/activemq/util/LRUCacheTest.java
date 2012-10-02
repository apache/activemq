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
package org.apache.activemq.util;

import static org.junit.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

public class LRUCacheTest {
    protected static final Logger LOG = LoggerFactory.getLogger(LRUCacheTest.class);
    
    @Test
    public void testResize() throws Exception {
        LRUCache<Long, Long> underTest = new LRUCache<Long, Long>(1000);
        
        Long count = new Long(0);
        long max = 0;
        for (; count < 27276827; count++) {
            long start = System.currentTimeMillis();
            if (!underTest.containsKey(count)) {
                underTest.put(count, count);
            }
            long duration = System.currentTimeMillis() - start;
            if (duration > max) {
                LOG.info("count: " + count + ", new max=" + duration);
                max = duration;
            }
            if (count % 100000000 == 0) {
                LOG.info("count: " + count + ", max=" + max);
            }
        }
        assertEquals("size is still in order", 1000, underTest.size());
    }
}
