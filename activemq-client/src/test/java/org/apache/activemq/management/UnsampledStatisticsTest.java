/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.activemq.management;

import static org.junit.Assert.*;

import org.junit.Test;

public class UnsampledStatisticsTest {

    @Test
    public void testUnsampledStatisticsEnabledTest() {
        UnsampledStatisticImpl<Long> longStatistic = new UnsampledStatisticImpl<>("longStat", "long", "A long statistic", Long.valueOf(0l));
        longStatistic.setEnabled(true);
        longStatistic.setValue(Long.MAX_VALUE);

        UnsampledStatisticImpl<String> stringStatistic = new UnsampledStatisticImpl<>("stringStat", "chars", "A string statistic", null);
        stringStatistic.setEnabled(true);
        stringStatistic.setValue("Hello World!");

        assertEquals("A long statistic", longStatistic.getDescription());
        assertEquals(Long.valueOf(0l), Long.valueOf(longStatistic.getLastSampleTime()));
        assertEquals("longStat", longStatistic.getName());
        assertEquals(Long.valueOf(0l), Long.valueOf(longStatistic.getStartTime()));
        assertEquals("long", longStatistic.getUnit());
        assertEquals(Long.valueOf(Long.MAX_VALUE), longStatistic.getValue());
        assertTrue(longStatistic.toString().contains("value: " + Long.MAX_VALUE));
        longStatistic.reset();
        assertEquals(Long.valueOf(0l), longStatistic.getValue());
        assertTrue(longStatistic.toString().contains("value: 0"));

        assertEquals("A string statistic", stringStatistic.getDescription());
        assertEquals(Long.valueOf(0l), Long.valueOf(stringStatistic.getLastSampleTime()));
        assertEquals("stringStat", stringStatistic.getName());
        assertEquals(Long.valueOf(0l), Long.valueOf(stringStatistic.getStartTime()));
        assertEquals("chars", stringStatistic.getUnit());
        assertEquals("Hello World!", stringStatistic.getValue());
        assertTrue(stringStatistic.toString().contains("value: Hello World!"));
        stringStatistic.reset();
        assertNull(stringStatistic.getValue());
        assertTrue(stringStatistic.toString().contains("value: null"));
    }

}
