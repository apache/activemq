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

import junit.framework.TestCase;

public abstract class StatisticTestSupport extends TestCase {

    /**
     * assert method used by the management related classes for its usecase.
     * 
     * @param counter
     * @param name
     * @param unit
     * @param description
     */
    protected void assertStatistic(StatisticImpl counter, String name, String unit, String description) {
        assertEquals(name, counter.getName());
        assertEquals(unit, counter.getUnit());
        assertEquals(description, counter.getDescription());
    }

    /**
     * assert method to determine last time vs the start time.
     * 
     * @param counter
     */
    protected void assertLastTimeNotStartTime(StatisticImpl counter) {
        assertTrue("Should not have start time the same as last sample time. Start time: "
                   + counter.getStartTime() + " lastTime: " + counter.getLastSampleTime(), counter
            .getStartTime() != counter.getLastSampleTime());
    }
}
