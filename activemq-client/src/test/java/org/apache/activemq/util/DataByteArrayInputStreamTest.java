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
import static org.junit.Assert.assertThrows;

import java.io.UTFDataFormatException;
import org.junit.Test;

/**
 * https://issues.apache.org/jira/browse/AMQ-1911
 * https://issues.apache.org/jira/browse/AMQ-8122
 */
public class DataByteArrayInputStreamTest {

    @Test
    public void testOneByteCharacters() throws Exception {
        testCodePointRange(0x000000, 0x00007F);
    }

    @Test
    public void testTwoBytesCharacters() throws Exception {
        testCodePointRange(0x000080, 0x0007FF);
    }

    @Test
    public void testThreeBytesCharacters() throws Exception {
        testCodePointRange(0x000800, 0x00FFFF);
    }

    @Test
    public void testFourBytesCharacters() throws Exception {
        testCodePointRange(0x010000, 0X10FFFF);
    }

    @Test
    public void testFourBytesCharacterEncodedAsBytes() throws Exception {
        // Currently ActiveMQ does not properly support 4-bytes UTF characters.
        // Ideally, this test should be failing. The current logic was kept as is
        // intentionally. See https://issues.apache.org/jira/browse/AMQ-8398.

        // 0xF0 0x80 0x80 0x80 (first valid 4-bytes character)
        testInvalidCharacterBytes(new byte[]{-16, -128, -128, -128}, 4);
        // 0xF7 0xBF 0xBF 0xBF (last valid 4-bytes character)
        testInvalidCharacterBytes(new byte[]{-9, -65, -65, -65}, 4);
    }


    private void testCodePointRange(int from, int to) throws Exception {
        StringBuilder accumulator = new StringBuilder();
        for (int codePoint = from; codePoint <= to; codePoint++) {
            String val = String.valueOf(Character.toChars(codePoint));
            accumulator.append(val);
            doMarshallUnMarshallValidation(val);
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
    public void testTwoBytesOutOfRangeCharacter() throws Exception {
        // 0xC0 0x7F
        testInvalidCharacterBytes(new byte[]{-64, 127}, 2);
        // 0xDF 0xC0
        testInvalidCharacterBytes(new byte[]{-33, -64}, 2);
    }

    @Test
    public void testThreeBytesOutOfRangeCharacter() throws Exception {
        // 0xE0 0x80 0x7F
        testInvalidCharacterBytes(new byte[]{-32, -128, 127}, 3);
        // 0xEF 0xBF 0xC0
        testInvalidCharacterBytes(new byte[]{-17, -65, -64}, 3);
    }

    @Test
    public void testFourBytesOutOfRangeCharacter() throws Exception {
        // 0xF0 0x80 0x80 0x7F
        testInvalidCharacterBytes(new byte[]{-16, -128, -128, 127}, 4);
        // 0xF7 0xBF 0xBF 0xC0
        testInvalidCharacterBytes(new byte[]{-9, -65, -65, -64}, 4);
    }

    private void testInvalidCharacterBytes(byte[] bytes, int encodedSize) throws Exception {
        // Java guarantees that strings are always UTF-8 compliant and valid,
        // any invalid sequence of bytes is either replaced or removed.
        // This test demonstrates that Java takes care about and does not allow
        // anything to break.
        String val = new String(bytes);
        doMarshallUnMarshallValidation(val);

        // However, a non-java client can send an invalid sequence of bytes.
        // Such data causes exceptions while unmarshalling.
        DataByteArrayOutputStream out = new DataByteArrayOutputStream();
        out.writeShort(encodedSize);
        out.write(bytes);
        out.close();

        DataByteArrayInputStream in = new DataByteArrayInputStream(out.getData());
        assertThrows(UTFDataFormatException.class, () -> in.readUTF());
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