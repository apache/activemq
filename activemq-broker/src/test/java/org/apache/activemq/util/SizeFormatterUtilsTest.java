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

import org.junit.Test;

public class SizeFormatterUtilsTest {

    @Test
    public void bTest() {
        assertEquals("1 B", SizeFormatterUtils.humanReadableBytes(1));
        assertEquals("750 B", SizeFormatterUtils.humanReadableBytes(750));
    }
    
    @Test
    public void kbTest() {
        assertEquals("1 KiB", SizeFormatterUtils.humanReadableBytes(1024));
        assertEquals("1.5 KiB", SizeFormatterUtils.humanReadableBytes(1536));
    }

    @Test
    public void mbTest() {
        assertEquals("1 MiB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1024, 2)));
        assertEquals("37 MiB", SizeFormatterUtils.humanReadableBytes(38797312));
        assertEquals("100.03 MiB", SizeFormatterUtils.humanReadableBytes(104889057));
    }

    @Test
    public void gbTest() {
        assertEquals("1 GiB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1024, 3)));
        assertEquals("2 GiB",
                SizeFormatterUtils.humanReadableBytes(Integer.MAX_VALUE));
    }

    @Test
    public void tbTest() {
        assertEquals("1 TiB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1024, 4)));
        assertEquals("30 TiB",
                SizeFormatterUtils.humanReadableBytes(30 * (long) Math.pow(1024, 4)));
    }

    @Test
    public void pbTest() {
        assertEquals("1 PiB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1024, 5)));
        assertEquals("2 PiB",
                SizeFormatterUtils.humanReadableBytes(2 * (long) Math.pow(1024, 5)));
    }

    @Test
    public void ebTest() {
        assertEquals("2 EiB",
                SizeFormatterUtils.humanReadableBytes(2 * (long) Math.pow(1024, 6)));
    }
    
    @Test
    public void bSiTest() {
        assertEquals("1 B", SizeFormatterUtils.humanReadableBytes(1, true));
        assertEquals("750 B", SizeFormatterUtils.humanReadableBytes(750, true));
    }
    
    @Test
    public void kbSiTest() {
        assertEquals("1 KB", SizeFormatterUtils.humanReadableBytes(1000, true));
        assertEquals("1.5 KB", SizeFormatterUtils.humanReadableBytes(1500, true));
    }

    @Test
    public void mbSiTest() {
        assertEquals("1 MB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1000, 2), true));
        assertEquals("38 MB", SizeFormatterUtils.humanReadableBytes(38000000, true));
    }

    @Test
    public void gbSiTest() {
        assertEquals("1 GB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1000, 3), true));
        assertEquals("2.147 GB",
                SizeFormatterUtils.humanReadableBytes(Integer.MAX_VALUE, true));
    }

    @Test
    public void tbSiTest() {
        assertEquals("1 TB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1000, 4), true));
        assertEquals("30 TB",
                SizeFormatterUtils.humanReadableBytes(30 * (long) Math.pow(1000, 4), true));
    }

    @Test
    public void pbSiTest() {
        assertEquals("1 PB",
                SizeFormatterUtils.humanReadableBytes((long) Math.pow(1000, 5), true));
        assertEquals("2 PB",
                SizeFormatterUtils.humanReadableBytes(2 * (long) Math.pow(1000, 5), true));
    }

    @Test
    public void ebSiTest() {
        assertEquals("2 EB",
                SizeFormatterUtils.humanReadableBytes(2 * (long) Math.pow(1000, 6), true));
    }

}
