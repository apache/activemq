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


public class CountStatisticTest extends StatisticTestSupport {
    
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(CountStatisticTest.class);

    /**
     * Use case for CountStatisticImple class.
     * @throws Exception
     */
    public void testStatistic() throws Exception {
        CountStatisticImpl stat = new CountStatisticImpl("myCounter", "seconds", "myDescription");
        stat.setEnabled(true);
        assertStatistic(stat, "myCounter", "seconds", "myDescription");

        assertEquals(0, stat.getCount());

        stat.increment();
        assertEquals(1, stat.getCount());

        stat.increment();
        assertEquals(2, stat.getCount());

        stat.decrement();
        assertEquals(1, stat.getCount());

        Thread.sleep(500);

        stat.increment();

        assertLastTimeNotStartTime(stat);

        log.info("Counter is: " + stat);

        stat.reset();

        assertEquals(0, stat.getCount());
    }
}
