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
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class StringArrayConverterTest {

    @Test
    public void testConvertToStringArray() throws Exception {
        assertNull(StringArrayConverter.convertToStringArray(null));
        assertNull(StringArrayConverter.convertToStringArray(""));

        String[] array = StringArrayConverter.convertToStringArray("foo");
        assertEquals(1, array.length);
        assertEquals("foo", array[0]);

        array = StringArrayConverter.convertToStringArray("foo,bar");
        assertEquals(2, array.length);
        assertEquals("foo", array[0]);
        assertEquals("bar", array[1]);

        array = StringArrayConverter.convertToStringArray("foo,bar,baz");
        assertEquals(3, array.length);
        assertEquals("foo", array[0]);
        assertEquals("bar", array[1]);
        assertEquals("baz", array[2]);
    }

    @Test
    public void testConvertToString() throws Exception {
        assertEquals(null, StringArrayConverter.convertToString(null));
        assertEquals(null, StringArrayConverter.convertToString(new String[]{}));
        assertEquals("", StringArrayConverter.convertToString(new String[]{""}));
        assertEquals("foo", StringArrayConverter.convertToString(new String[]{"foo"}));
        assertEquals("foo,bar", StringArrayConverter.convertToString(new String[]{"foo", "bar"}));
        assertEquals("foo,bar,baz", StringArrayConverter.convertToString(new String[]{"foo", "bar", "baz"}));
    }
}
