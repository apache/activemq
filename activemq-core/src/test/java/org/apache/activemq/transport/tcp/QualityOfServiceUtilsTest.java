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
package org.apache.activemq.transport.tcp;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;

public class QualityOfServiceUtilsTest extends TestCase {
    /**
     * Keeps track of the value that the System has set for the ECN bits, which
     * should not be overridden when Differentiated Services is set, but may be
     * overridden when Type of Service is set.
     */
    private int ECN;

    protected void setUp() throws Exception {
        Socket socket = new Socket();
        ECN = socket.getTrafficClass() & Integer.parseInt("00000011", 2);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testValidDiffServIntegerValues() {
        int[] values = {0, 1, 32, 62, 63};
        for (int val : values) {
            testValidDiffServIntegerValue(val);
        }
    }

    public void testInvalidDiffServIntegerValues() {
        int[] values = {-2, -1, 64, 65};
        for (int val : values) {
            testInvalidDiffServIntegerValue(val);
        }
    }

    public void testValidDiffServNames() {
        Map<String, Integer> namesToExpected = new HashMap<String, Integer>();
        namesToExpected.put("EF", Integer.valueOf("101110", 2));
        namesToExpected.put("AF11", Integer.valueOf("001010", 2));
        namesToExpected.put("AF12", Integer.valueOf("001100", 2));
        namesToExpected.put("AF13", Integer.valueOf("001110", 2));
        namesToExpected.put("AF21", Integer.valueOf("010010", 2));
        namesToExpected.put("AF22", Integer.valueOf("010100", 2));
        namesToExpected.put("AF23", Integer.valueOf("010110", 2));
        namesToExpected.put("AF31", Integer.valueOf("011010", 2));
        namesToExpected.put("AF32", Integer.valueOf("011100", 2));
        namesToExpected.put("AF33", Integer.valueOf("011110", 2));
        namesToExpected.put("AF41", Integer.valueOf("100010", 2));
        namesToExpected.put("AF42", Integer.valueOf("100100", 2));
        namesToExpected.put("AF43", Integer.valueOf("100110", 2));
        for (String name : namesToExpected.keySet()) {
            testValidDiffServName(name, namesToExpected.get(name));
        }
    }

    public void testInvalidDiffServNames() {
        String[] names = {"hello_world", "", "abcd"};
        for (String name : names) {
            testInvalidDiffServName(name);
        }
    }

    private void testValidDiffServName(String name, int expected) {
        int dscp = -1;
        try {
            dscp = QualityOfServiceUtils.getDSCP(name);
        } catch (IllegalArgumentException e) {
            fail("IllegalArgumentException thrown for valid Differentiated "
                 + " Services name: " + name);
        }
        // Make sure it adjusted for any system ECN values.
        assertEquals("Incorrect Differentiated Services Code Point "  + dscp
            + " returned for name " + name + ".", ECN | (expected << 2), dscp);
    }

    private void testInvalidDiffServName(String name) {
        try {
            int dscp = QualityOfServiceUtils.getDSCP(name);
            fail("No IllegalArgumentException thrown for invalid Differentiated"
                 + " Services value: " + name + ".");
        } catch (IllegalArgumentException e) {
        }
    }
    
    private void testValidDiffServIntegerValue(int val) {
        try {
            int dscp = QualityOfServiceUtils.getDSCP(Integer.toString(val));
            // Make sure it adjusted for any system ECN values.
            assertEquals("Incorrect Differentiated Services Code Point "
                + "returned for value " + val + ".", ECN | (val << 2), dscp);
        } catch (IllegalArgumentException e) {
            fail("IllegalArgumentException thrown for valid Differentiated "
                 + "Services value " + val);
        }
    }

    private void testInvalidDiffServIntegerValue(int val) {
        try {
            int dscp = QualityOfServiceUtils.getDSCP(Integer.toString(val));
            fail("No IllegalArgumentException thrown for invalid "
                + "Differentiated Services value " + val + ".");
        } catch (IllegalArgumentException expected) {
        }
    }

    public void testValidTypeOfServiceValues() {
        int[] values = {0, 1, 32, 100, 255};
        for (int val : values) {
            testValidTypeOfServiceValue(val);
        }
    }

    public void testInvalidTypeOfServiceValues() {
        int[] values = {-2, -1, 256, 257};
        for (int val : values) {
            testInvalidTypeOfServiceValue(val);
        }
    }

    private void testValidTypeOfServiceValue(int val) {
        try {
            int typeOfService = QualityOfServiceUtils.getToS(val);
            assertEquals("Incorrect Type of Services value returned for " + val
                + ".", val, typeOfService);
        } catch (IllegalArgumentException e) {
            fail("IllegalArgumentException thrown for valid Type of Service "
                 + "value " + val + ".");
        }
    }

    private void testInvalidTypeOfServiceValue(int val) {
        try {
            int typeOfService = QualityOfServiceUtils.getToS(val);
            fail("No IllegalArgumentException thrown for invalid "
                + "Type of Service value " + val + ".");
        } catch (IllegalArgumentException expected) {
        }
    }
}
