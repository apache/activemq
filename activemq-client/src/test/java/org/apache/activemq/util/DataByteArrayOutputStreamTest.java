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

import java.io.IOException;

import org.junit.Test;

public class DataByteArrayOutputStreamTest {

    /**
     * This test case assumes that an ArrayIndexOutOfBoundsException will be thrown when the buffer fails to resize
     * @throws IOException
     */
    @Test
    public void testResize() throws IOException {
        int initSize = 64;
        DataByteArrayOutputStream out = new DataByteArrayOutputStream();

        fillOut(out, initSize);
        // Should resized here
        out.writeBoolean(true);

        fillOut(out, initSize);
        // Should resized here
        out.writeByte(1);

        fillOut(out, initSize);
        // Should resized here
        out.writeBytes("test");

        fillOut(out, initSize);
        // Should resized here
        out.writeChar('C');

        fillOut(out, initSize);
        // Should resized here
        out.writeChars("test");

        fillOut(out, initSize);
        // Should resized here
        out.writeDouble(3.1416);

        fillOut(out, initSize);
        // Should resized here
        out.writeFloat((float)3.1416);

        fillOut(out, initSize);
        // Should resized here
        out.writeInt(12345);

        fillOut(out, initSize);
        // Should resized here
        out.writeLong(12345);

        fillOut(out, initSize);
        // Should resized here
        out.writeShort(1234);

        fillOut(out, initSize);
        // Should resized here
        out.writeUTF("test");

        fillOut(out, initSize);
        // Should resized here
        out.write(1234);

        fillOut(out, initSize);
        // Should resized here
        out.write(new byte[10], 5, 5);

        fillOut(out, initSize);
        // Should resized here
        out.write(new byte[10]);
    }

    /**
     * This method restarts the stream to the init size, and fills it up with data
     * @param out
     * @param size
     * @throws IOException
     */
    public void fillOut(DataByteArrayOutputStream out, int size) throws IOException {
        out.restart(size);
        out.write(new byte[size]);
    }
}
