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

/**
 * https://issues.apache.org/jira/browse/AMQ-1911
 * https://issues.apache.org/jira/browse/AMQ-8122
 */
public class DataByteArrayInputStreamTest {

    @Test
    public void testOneByteCharacters() throws Exception {
        testCodePointRange(0x0000, 0x007F);
    }

    @Test
    public void testTwoBytesCharacters() throws Exception {
        testCodePointRange(0x0080, 0x07FF);
    }

    @Test
    public void testThreeBytesCharacters() throws Exception {
        testCodePointRange(0x0800, 0xFFFF);
    }

    @Test
    public void testFourBytesCharacters() throws Exception {
        testCodePointRange(0x10000, 0X10FFFF);
    }

    private void testCodePointRange(int from, int to) throws Exception {
        StringBuilder accumulator = new StringBuilder();
        for (int codePoint = from; codePoint <= to; codePoint++) {
            if (Character.isDefined(codePoint)
                // a single surrogate code point does not represent a real character
                && !Character.isHighSurrogate((char) codePoint)
                && !Character.isLowSurrogate((char) codePoint)) {
                    String val = String.valueOf(Character.toChars(codePoint));
                    accumulator.append(val);
                    doMarshallUnMarshallValidation(val);
            }
        }

        // truncate string to last 20k characters
        if (accumulator.length() > 20_000) {
            doMarshallUnMarshallValidation(accumulator.substring(
                accumulator.length() - 20_000));
        } else {
            doMarshallUnMarshallValidation(accumulator.toString());
        }
    }

    private void doMarshallUnMarshallValidation(String value) throws Exception {
        DataByteArrayOutputStream out = new DataByteArrayOutputStream();
        out.writeUTF(value);
        out.close();

        DataByteArrayInputStream in = new DataByteArrayInputStream(out.getData());
        String readBack = in.readUTF();

        assertEquals(value, readBack);
    }

    @Test
    public void testReadLong() throws Exception {
        DataByteArrayOutputStream out = new DataByteArrayOutputStream(8);
        out.writeLong(Long.MAX_VALUE);
        out.close();

        DataByteArrayInputStream in = new DataByteArrayInputStream(out.getData());
        long readBack = in.readLong();
        assertEquals(Long.MAX_VALUE, readBack);
    }
}
