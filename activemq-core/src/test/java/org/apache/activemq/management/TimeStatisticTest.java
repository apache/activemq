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


public class TimeStatisticTest extends StatisticTestSupport {
    
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(TimeStatisticTest.class);

    /**
     * Use case for TimeStatisticImpl class.
     * @throws Exception
     */
    public void testStatistic() throws Exception {
        TimeStatisticImpl stat = new TimeStatisticImpl("myTimer", "millis", "myDescription");
        assertStatistic(stat, "myTimer", "millis", "myDescription");

        assertEquals(0, stat.getCount());

        stat.addTime(100);
        assertEquals(1, stat.getCount());
        assertEquals(100, stat.getMinTime());
        assertEquals(100, stat.getMaxTime());

        stat.addTime(403);
        assertEquals(2, stat.getCount());
        assertEquals(100, stat.getMinTime());
        assertEquals(403, stat.getMaxTime());

        stat.addTime(50);
        assertEquals(3, stat.getCount());
        assertEquals(50, stat.getMinTime());
        assertEquals(403, stat.getMaxTime());


        assertEquals(553, stat.getTotalTime());

        Thread.sleep(500);

        stat.addTime(10);

        assertLastTimeNotStartTime(stat);

        log.info("Stat is: " + stat);

        stat.reset();

        assertEquals(0, stat.getCount());
        assertEquals(0, stat.getMinTime());
        assertEquals(0, stat.getMaxTime());
        assertEquals(0, stat.getTotalTime());

        stat.addTime(100);
        assertEquals(1, stat.getCount());
        assertEquals(100, stat.getMinTime());
        assertEquals(100, stat.getMaxTime());
        assertEquals(100, stat.getTotalTime());

    }
}
