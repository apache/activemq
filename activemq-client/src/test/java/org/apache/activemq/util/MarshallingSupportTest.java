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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Properties;

import org.junit.Test;

public class MarshallingSupportTest {

    /**
     * Test method for
     * {@link org.apache.activemq.util.MarshallingSupport#propertiesToString(java.util.Properties)}.
     *
     * @throws Exception
     */
    @Test
    public void testPropertiesToString() throws Exception {
        Properties props = new Properties();
        for (int i = 0; i < 10; i++) {
            String key = "key" + i;
            String value = "value" + i;
            props.put(key, value);
        }
        String str = MarshallingSupport.propertiesToString(props);
        Properties props2 = MarshallingSupport.stringToProperties(str);
        assertEquals(props, props2);
    }

    @Test
    public void testReadWriteUtf8() throws Exception {
        byte[] bytes = {0, 0, 0, 10, 33, -62, -82, -32, -79, -87, -16, -97, -103, -126};
        String str = "!®౩\uD83D\uDE42";

        DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
        String resultStr = MarshallingSupport.readUTF8(dataIn);
        assertEquals(str, resultStr);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dataOut = new DataOutputStream(baos);
        MarshallingSupport.writeUTF8(dataOut, str);
        byte[] resultBytes = baos.toByteArray();
        assertArrayEquals(bytes, resultBytes);
    }
}
