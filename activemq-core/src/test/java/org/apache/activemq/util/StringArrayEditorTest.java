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

import junit.framework.TestCase;

/**
 *
 */
public class StringArrayEditorTest extends TestCase {

    private StringArrayEditor editor = new StringArrayEditor();

    public void testConvertToStringArray() throws Exception {
        editor.setAsText(null);
        assertEquals(null, editor.getValue());
        editor.setAsText("");
        assertEquals(null, editor.getValue());

        editor.setAsText("foo");
        String[] array = (String[]) editor.getValue();
        assertEquals(1, array.length);
        assertEquals("foo", array[0]);

        editor.setAsText("foo,bar");
        array = (String[]) editor.getValue();
        assertEquals(2, array.length);
        assertEquals("foo", array[0]);
        assertEquals("bar", array[1]);

        editor.setAsText("foo,bar,baz");
        array = (String[]) editor.getValue();
        assertEquals("foo", array[0]);
        assertEquals("bar", array[1]);
        assertEquals("baz", array[2]);
    }

    public void testConvertToString() throws Exception {
        editor.setValue(null);
        assertEquals(null, editor.getAsText());

        editor.setValue(new String[]{});
        assertEquals(null, editor.getAsText());

        editor.setValue(new String[]{""});
        assertEquals("", editor.getAsText());

        editor.setValue(new String[]{"foo"});
        assertEquals("foo", editor.getAsText());

        editor.setValue(new String[]{"foo", "bar"});
        assertEquals("foo,bar", editor.getAsText());

        editor.setValue(new String[]{"foo", "bar", "baz"});
        assertEquals("foo,bar,baz", editor.getAsText());
    }

}
