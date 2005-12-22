/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.filter;

import junit.framework.TestCase;

public class DestinationPathTest extends TestCase {

    public void testPathParse() {
        assertParse("FOO", new String[]{"FOO"});
        assertParse("FOO.BAR", new String[]{"FOO", "BAR"});
        assertParse("FOO.*", new String[]{"FOO", "*"});
        assertParse("FOO.>", new String[]{"FOO", ">"});
        assertParse("FOO.BAR.XYZ", new String[]{"FOO", "BAR", "XYZ"});
        assertParse("FOO.BAR.", new String[]{"FOO", "BAR", ""});
    }

    protected void assertParse(String subject, String[] expected) {
        String[] path = DestinationPath.getDestinationPaths(subject);
        assertArrayEqual(subject, expected, path);
    }

    protected void assertArrayEqual(String message, Object[] expected, Object[] actual) {
        assertEquals(message + ". Array length", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(message + ". element: " + i, expected[i], actual[i]);
        }
    }
}
