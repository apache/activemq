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

package org.apache.activemq.transport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

/**
 * Tests for FrameSizeLimitedFilterInputStream
 */
public class FrameSizeLimitedFilterInputStreamTest {

    private static final int DEFAULT_TEST_PAYLOAD_SIZE = 256;

    private byte[] createPayload() {
        final byte[] data = new byte[DEFAULT_TEST_PAYLOAD_SIZE];

        for (int i = 0; i < DEFAULT_TEST_PAYLOAD_SIZE; ++i) {
            data[i] = (byte) i;
        }

        return data;
    }

    @Test
    public void testCreate() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[2048]);

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1024, bais)) {
            assertTrue(stream.markSupported());
            assertEquals(1024, stream.available());
        }
    }

    @Test
    public void testCreateChecks() throws IOException {
        assertThrows(NullPointerException.class, () -> new FrameSizeLimitedFilterInputStream(1024, null));
        assertThrows(IllegalArgumentException.class, () -> new FrameSizeLimitedFilterInputStream(-1, new ByteArrayInputStream(new byte[0])));
    }

    @Test
    public void testCreateUnbound() throws IOException {
        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream()) {
            assertThrows(NullPointerException.class, () -> stream.read());
            assertThrows(NullPointerException.class, () -> stream.skip(1));
            assertThrows(NullPointerException.class, () -> stream.read(new byte[0]));
            assertThrows(NullPointerException.class, () -> stream.read(new byte[1], 0, 1));
        }
    }

    @Test
    public void testUnusableAfterClose() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[2048]);

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1024, bais)) {
            stream.close();

            assertThrows(IOException.class, () -> stream.read());
            assertThrows(IOException.class, () -> stream.skip(1));
            assertThrows(IOException.class, () -> stream.read(new byte[1]));
            assertThrows(IOException.class, () -> stream.read(new byte[1], 0, 1));

            assertFalse(stream.markSupported());

            // Should no-op and not throw
            stream.mark(1);
            stream.reset();
        }
    }

    @Test
    public void testReadByte() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(Byte.MAX_VALUE, bais)) {
            for (int i = 0; i < Byte.MAX_VALUE; ++i) {
                assertEquals(i, stream.read());
            }

            assertThrows(IOException.class, () -> stream.read());
        }
    }

    @Test
    public void testReadFailsAfterClosed() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(Byte.MAX_VALUE, bais)) {
            stream.close();

            assertThrows(IOException.class, () -> stream.read());
            assertThrows(IOException.class, () -> stream.read(new byte[10]));
            assertThrows(IOException.class, () -> stream.read(new byte[10], 0, 10));
        }
    }

    @Test
    public void testReadByteAndResetAvailable() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1, bais)) {
            assertEquals(0, stream.read());
            assertThrows(IOException.class, () -> stream.read());

            stream.resetAvailable();

            assertEquals(1, stream.read());
            assertThrows(IOException.class, () -> stream.read());

            stream.resetAvailable(2);

            assertEquals(2, stream.read());
            assertEquals(3, stream.read());
            assertThrows(IOException.class, () -> stream.read());
        }
    }

    @Test
    public void testReadBytes() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(Byte.MAX_VALUE, bais)) {
            final byte[] sink = new byte[Byte.MAX_VALUE];

            assertEquals(Byte.MAX_VALUE, stream.read(sink));

            for (int i = 0; i < Byte.MAX_VALUE; ++i) {
                assertEquals(i, sink[i]);
            }

            assertThrows(IOException.class, () -> stream.read());
        }
    }

    @Test
    public void testReadBytesAndResetAvailable() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());
        final byte[] sink = new byte[1];

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1, bais)) {
            assertEquals(1, stream.read(sink));
            assertEquals(0, sink[0]);
            assertThrows(IOException.class, () -> stream.read());

            stream.resetAvailable();

            assertEquals(1, stream.read(sink));
            assertEquals(1, sink[0]);
            assertThrows(IOException.class, () -> stream.read());

            stream.resetAvailable(2);

            assertEquals(1, stream.read(sink));
            assertEquals(2, sink[0]);
            assertEquals(1, stream.read(sink));
            assertEquals(3, sink[0]);
            assertThrows(IOException.class, () -> stream.read());
        }
    }

    @Test
    public void testReadBytesIndexed() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(Byte.MAX_VALUE, bais)) {
            final byte[] sink = new byte[Byte.MAX_VALUE];

            assertEquals(Byte.MAX_VALUE, stream.read(sink, 0, sink.length));

            for (int i = 0; i < Byte.MAX_VALUE; ++i) {
                assertEquals(i, sink[i]);
            }

            assertThrows(IOException.class, () -> stream.read(sink, 0, sink.length));
        }
    }

    @Test
    public void testReadBytesIndexedAndResetAvailable() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());
        final byte[] sink = new byte[1];

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1, bais)) {
            assertEquals(1, stream.read(sink, 0, sink.length));
            assertEquals(0, sink[0]);
            assertThrows(IOException.class, () -> stream.read());

            stream.resetAvailable();

            assertEquals(1, stream.read(sink, 0, sink.length));
            assertEquals(1, sink[0]);
            assertThrows(IOException.class, () -> stream.read());

            stream.resetAvailable(2);

            assertEquals(1, stream.read(sink, 0, sink.length));
            assertEquals(2, sink[0]);
            assertEquals(1, stream.read(sink, 0, sink.length));
            assertEquals(3, sink[0]);
            assertThrows(IOException.class, () -> stream.read());
        }
    }

    @Test
    public void testSkipBytes() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1, bais)) {
            assertEquals(1, stream.skip(1));
            assertThrows(IOException.class, () -> stream.skip(1));

            stream.resetAvailable();

            assertEquals(1, stream.skip(1));
            assertThrows(IOException.class, () -> stream.skip(10));

            stream.resetAvailable(2);

            assertEquals(2, stream.skip(2));
            assertThrows(IOException.class, () -> stream.skip(100));

            stream.resetAvailable();

            assertEquals(4, stream.read());
            assertEquals(5, stream.read());

            assertThrows(IOException.class, () -> stream.skip(1));
        }
    }

    @Test
    public void testSkipNegativeThrows() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1, bais)) {
            assertEquals(1, stream.available());
            assertEquals(0, stream.skip(-1));
            assertEquals(1, stream.available());
        }
    }

    @Test
    public void testSkipMassiveSize() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload()) {

            @Override
            public long skip(long amount) {
                return amount;
            }
        };

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(Integer.MAX_VALUE, bais)) {
            assertEquals(Integer.MAX_VALUE, stream.skip(Long.MAX_VALUE));

            assertThrows(IOException.class, () -> stream.skip(1));
            assertThrows(IOException.class, () -> stream.read());
            assertThrows(IOException.class, () -> stream.read(new byte[1]));
            assertThrows(IOException.class, () -> stream.read(new byte[1], 0, 1));
        }
    }

    @Test
    public void testSkipBytesPastConfiguredAvailable() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(1, bais)) {
            assertThrows(IOException.class, () -> stream.skip(2));

            assertEquals(1, stream.skip(1));

            assertThrows(IOException.class, () -> stream.skip(1));
        }
    }

    @Test
    public void testBasicMark() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        assertTrue(bais.markSupported());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(10, bais)) {
            assertTrue(stream.markSupported());

            stream.mark(1);
            assertEquals(0, stream.read());
            assertEquals(9, stream.available());

            stream.reset();
            assertEquals(10, stream.available());
            assertEquals(0, stream.read());
            assertEquals(9, stream.available());

            stream.reset(); // Mark wasn't called
            assertEquals(9, stream.available());
        }
    }

    @Test
    public void testMarkZeroIsIgnored() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        assertTrue(bais.markSupported());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(10, bais)) {
            assertTrue(stream.markSupported());

            stream.mark(0);
            assertEquals(0, stream.read());
            assertEquals(9, stream.available());

            stream.reset();
            assertEquals(9, stream.available());
            assertEquals(1, stream.read());
            assertEquals(8, stream.available());

            stream.reset(); // Mark wasn't called
            assertEquals(8, stream.available());
        }
    }

    @Test
    public void testLastMarkWins() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        assertTrue(bais.markSupported());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(10, bais)) {
            assertTrue(stream.markSupported());

            stream.mark(1);
            stream.mark(2);
            stream.mark(3);
            stream.mark(4);

            final byte[] read1 = new byte[4];
            final byte[] read2 = new byte[4];

            stream.read(read1);
            assertEquals(6, stream.available());
            stream.reset();
            assertEquals(10, stream.available());
            stream.read(read2);

            assertArrayEquals(read1, read2);
        }
    }
    @Test
    public void testReadPastMarkClearsMark() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        assertTrue(bais.markSupported());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(10, bais)) {
            assertTrue(stream.markSupported());

            stream.mark(3);

            assertEquals(3, stream.read(new byte[3]));
            assertEquals(3, stream.read()); // Past mark.
            assertEquals(6, stream.available());

            stream.reset(); // Should have no affect

            assertEquals(6, stream.available());
            assertEquals(4, stream.read());

            assertThrows(IOException.class, () -> stream.read(new byte[10], 0, 10));

            assertEquals(5, stream.available());
            assertEquals(5, stream.read());
        }
    }

    @Test
    public void testMarkNotSupported() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload()) {

            @Override
            public boolean markSupported() {
                return false;
            }
        };

        assertFalse(bais.markSupported());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(10, bais)) {
            assertFalse(stream.markSupported());

            stream.mark(1);
            assertEquals(0, stream.read());
            assertEquals(9, stream.available());

            stream.reset();
            assertEquals(9, stream.available());
            assertEquals(1, stream.read());
            assertEquals(8, stream.available());

            stream.reset();
            assertEquals(8, stream.available());

            final byte[] first = new byte[8];

            stream.mark(8);
            stream.read(first);
            assertEquals(0, stream.available());
            stream.reset();
            assertEquals(0, stream.available());

            stream.resetAvailable(10);

            final byte[] second = new byte[10];

            stream.mark(10);
            stream.read(second);
            assertEquals(0, stream.available());
            stream.reset();
            assertEquals(0, stream.available());
        }
    }

    @Test
    public void testResetToNewStream() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[2048]);

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(2048, bais)) {
            assertTrue(stream.markSupported());
            assertEquals(2048, stream.available());

            final ByteArrayInputStream nextStream = new ByteArrayInputStream(new byte[1024]) {

                @Override
                public boolean markSupported() {
                    return false;
                }
            };

            assertFalse(nextStream.markSupported());

            stream.resetAvailable(nextStream, 1024);

            assertFalse(stream.markSupported());
            assertEquals(1024, stream.available());
        }
    }

    @Test
    public void testResetWithinSameStream() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(createPayload());

        try (FrameSizeLimitedFilterInputStream stream = new FrameSizeLimitedFilterInputStream(DEFAULT_TEST_PAYLOAD_SIZE, bais)) {
            assertTrue(stream.markSupported());
            assertEquals(DEFAULT_TEST_PAYLOAD_SIZE, stream.available());

            assertEquals(0, stream.read());

            assertThrows(IllegalArgumentException.class, () -> stream.resetAvailable(-1));
            stream.resetAvailable(1);

            assertEquals(1, stream.read());
            assertThrows(IOException.class, () -> stream.read());

            stream.resetAvailable(2);

            assertEquals(2, stream.read());
            assertEquals(3, stream.read());
            assertThrows(IOException.class, () -> stream.read());
        }
    }
}
