/*
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

package org.apache.activemq.command;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;

public class WireFormatInfoTest {

    private final OpenWireFormat wireFormat = new OpenWireFormat();

    @Test
    public void testWireFormatLimits() throws Exception {
        WireFormatInfo wfi = new WireFormatInfo();
        byte[] bytes = "a".repeat(WireFormatInfo.MAX_PROPERTY_BUFFER_SIZE).getBytes();

        // We allow up to 64 properties with 512 bytes
        for (int i = 0; i < WireFormatInfo.MAX_PROPERTY_SIZE; i++) {
            wfi.setProperty("prop" + i, bytes);
        }

        // Marshal/unmarshal so we have only the marshaled properties stored
        wfi = marshal(wfi);
        assertMarshaled(wfi);

        // Trigger unmarshal and verify it works
        assertEquals(WireFormatInfo.MAX_PROPERTY_SIZE, wfi.getProperties().size());
        assertArrayEquals(bytes, (byte[])wfi.getProperties().get("prop0"));
    }

    @Test
    public void testWireFormatInfoTooManyProperties() throws Exception {
        WireFormatInfo wfi = new WireFormatInfo();

        // We allow up to 64 properties, so add too many
        for (int i = 0; i < WireFormatInfo.MAX_PROPERTY_SIZE + 1; i++) {
            wfi.setProperty("prop" + i, "test");
        }

        // Marshal/unmarshal so we have only the marshaled properties stored
        wfi = marshal(wfi);
        assertMarshaled(wfi);

        try {
            // Trigger unmarshaling the properties. This should throw an error because
            // We have too many properties in the map
            wfi.getProperties();
            fail("should have exception");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("map is larger than the allowed size"));
        }
    }

    @Test
    public void testWireFormatInfoTooLargeBytes() throws Exception {
        testWireFormatInfoValidation("a".repeat(WireFormatInfo.MAX_PROPERTY_BUFFER_SIZE + 1).getBytes(),
                "Max buffer size: 512 exceeded, size: 513");
    }

    @Test
    public void testWireFormatInfoTooLargeString() throws Exception {
        testWireFormatInfoValidation("a".repeat(WireFormatInfo.MAX_PROPERTY_BUFFER_SIZE + 1),
                "Max buffer size: 512 exceeded, size: 513");
    }

    @Test
    public void testWireFormatInfoTooLargeList() throws Exception {
        // buffer is too large but we also don't allow lists
        testWireFormatInfoValidation("a".repeat(WireFormatInfo.MAX_PROPERTY_BUFFER_SIZE + 1).chars()
                .mapToObj(c -> (char) c)
                .collect(Collectors.toList()), "Max unmarshaling depth: 0 exceeded");
    }

    // Make sure nested collections not allowed
    @Test
    public void testWireFormatInfoNestedList() throws Exception {
        // we don't allow any lists at all
        testWireFormatInfoValidation(List.of("blocked"), "Max unmarshaling depth: 0 exceeded");

        // also check nested
        List<List<String>> list = List.of(List.of("blocked"));
        testWireFormatInfoValidation(list, "Max unmarshaling depth: 0 exceeded");
    }

    @Test
    public void testWireFormatInfoTooLargeMap() throws Exception {
        Map<String,Boolean> map = new HashMap<>();
        for (int i = 0; i < WireFormatInfo.MAX_PROPERTY_BUFFER_SIZE + 1; i++) {
            map.put("prop" + i, true);
        }
        // too large of a map but we also block maps in general
        testWireFormatInfoValidation(map, "Max unmarshaling depth: 0 exceeded");
    }

    @Test
    public void testWireFormatInfoNestedMap() throws Exception {
        Map<String,Map<String,String>> map = Map.of("blocked", Map.of("a", "b"));
        testWireFormatInfoValidation(map, "Max unmarshaling depth: 0 exceeded");
    }

    @Test
    public void testWireFormatInfoNestedMapOfList() throws Exception {
        Map<String,List<String>> map = Map.of("blocked", List.of("test"));
        testWireFormatInfoValidation(map, "Max unmarshaling depth: 0 exceeded");
    }

    @Test
    public void testWireFormatInfoNestedListOfMap() throws Exception {
        List<Map<String,String>> list = List.of(Map.of("blocked", "blocked"));
        testWireFormatInfoValidation(list, "Max unmarshaling depth: 0 exceeded");
    }

    // test for MarshallingSupport.BIG_STRING_TYPE
    // Anythin over Short.MAX_VALUE / 4 will be a big string type
    @Test
    public void testWireFormatInfoTooLargeBigString() throws Exception {
        testWireFormatInfoValidation("a".repeat((Short.MAX_VALUE / 4) + 1),
                "Max buffer size: 512 exceeded, size: 8192");
    }

    private void testWireFormatInfoValidation(Object buffer, String error) throws Exception {
        WireFormatInfo wfi = new WireFormatInfo();

        // We allow up to 1024 buffer size, add an extra byte
        wfi.setProperty("prop", buffer);

        // Marshal/unmarshal so we have only the marshaled properties stored
        wfi = marshal(wfi);
        assertMarshaled(wfi);

        try {
            // Trigger unmarshaling the properties. This should throw an error because
            // the buffer is too large
            wfi.getProperties();
            fail("should have exception");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains(error));
        }
    }

    // Marshal/unmarshal so we have only the marshaled properties stored
    private WireFormatInfo marshal(WireFormatInfo wfi) throws IOException {
        ByteSequence s = wireFormat.marshal(wfi);
        return (WireFormatInfo) wireFormat.unmarshal(s);
    }

    private static void assertMarshaled(WireFormatInfo wfi) {
        assertNull(wfi.properties);
        assertNotNull(wfi.marshalledProperties);
    }

}
