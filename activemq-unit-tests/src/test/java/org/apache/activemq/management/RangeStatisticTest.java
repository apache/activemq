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
package org.apache.activemq.management;


public class RangeStatisticTest extends StatisticTestSupport {
    
    private static final org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
            .getLog(RangeStatisticTest.class);

    /**
     * Use case for RangeStatisticImpl class.
     * @throws Exception
     */
    public void testStatistic() throws Exception {
        RangeStatisticImpl stat = new RangeStatisticImpl("myRange", "millis", "myDescription");
        assertStatistic(stat, "myRange", "millis", "myDescription");

        assertRangeStatistic(stat);
    }

    protected void assertRangeStatistic(RangeStatisticImpl stat) throws InterruptedException {
        assertEquals(0, stat.getCurrent());
        assertEquals(0, stat.getLowWaterMark());
        assertEquals(0, stat.getHighWaterMark());

        stat.setCurrent(100);
        assertEquals(100, stat.getCurrent());
        assertEquals(100, stat.getLowWaterMark());
        assertEquals(100, stat.getHighWaterMark());

        stat.setCurrent(50);
        assertEquals(50, stat.getCurrent());
        assertEquals(50, stat.getLowWaterMark());
        assertEquals(100, stat.getHighWaterMark());

        stat.setCurrent(200);
        assertEquals(200, stat.getCurrent());
        assertEquals(50, stat.getLowWaterMark());
        assertEquals(200, stat.getHighWaterMark());

        Thread.sleep(500);

        stat.setCurrent(10);
        assertEquals(10, stat.getCurrent());
        assertEquals(10, stat.getLowWaterMark());
        assertEquals(200, stat.getHighWaterMark());

        assertLastTimeNotStartTime(stat);

        LOG.info("Stat is: " + stat);

        stat.reset();

        assertEquals(0, stat.getCurrent());
        assertEquals(0, stat.getLowWaterMark());
        assertEquals(0, stat.getHighWaterMark());

        stat.setCurrent(100);
        assertEquals(100, stat.getCurrent());
        assertEquals(100, stat.getLowWaterMark());
        assertEquals(100, stat.getHighWaterMark());
    }
}
