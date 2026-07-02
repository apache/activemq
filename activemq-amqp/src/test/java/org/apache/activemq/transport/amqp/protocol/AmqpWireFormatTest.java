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
package org.apache.activemq.transport.amqp.protocol;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.transport.amqp.AmqpHeader;
import org.apache.activemq.transport.amqp.AmqpWireFormat;
import org.apache.activemq.transport.amqp.AmqpWireFormat.ResetListener;
import org.apache.activemq.util.ByteSequence;
import org.junit.Before;
import org.junit.Test;

public class AmqpWireFormatTest {

    private final AmqpWireFormat wireFormat = new AmqpWireFormat();

    @Before
    public void setUp() throws Exception {
        wireFormat.setMaxFrameSize(AmqpWireFormat.DEFAULT_MAX_FRAME_SIZE);
    }

    @Test
    public void testWhenSaslNotAllowedNonSaslHeaderIsInvliad() {
        wireFormat.setAllowNonSaslConnections(false);

        AmqpHeader nonSaslHeader = new AmqpHeader();
        assertFalse(wireFormat.isHeaderValid(nonSaslHeader, false));
        AmqpHeader saslHeader = new AmqpHeader();
        saslHeader.setProtocolId(3);
        assertTrue(wireFormat.isHeaderValid(saslHeader, false));
    }

    @Test
    public void testWhenSaslAllowedNonSaslHeaderIsValid() {
        wireFormat.setAllowNonSaslConnections(true);

        AmqpHeader nonSaslHeader = new AmqpHeader();
        assertTrue(wireFormat.isHeaderValid(nonSaslHeader, false));
        AmqpHeader saslHeader = new AmqpHeader();
        saslHeader.setProtocolId(3);
        assertTrue(wireFormat.isHeaderValid(saslHeader, false));
    }

    @Test
    public void testNonSaslHeaderAfterSaslAuthenticationIsAccepted() {
        wireFormat.setAllowNonSaslConnections(false);

        AmqpHeader nonSaslHeader = new AmqpHeader();
        assertTrue(wireFormat.isHeaderValid(nonSaslHeader, true));
        AmqpHeader saslHeader = new AmqpHeader();
        saslHeader.setProtocolId(3);
        assertTrue(wireFormat.isHeaderValid(saslHeader, false));
    }

    @Test
    public void testMagicResetListener() throws Exception {
        final AtomicBoolean reset = new AtomicBoolean();

        wireFormat.setProtocolResetListener(new ResetListener() {

            @Override
            public void onProtocolReset() {
                reset.set(true);
            }
        });

        wireFormat.resetMagicRead();
        assertTrue(reset.get());
    }


    @Test
    public void testNegativeFrameSize() throws Exception {
        AmqpHeader inputHeader = new AmqpHeader();
        wireFormat.unmarshal(new ByteSequence(inputHeader.getBuffer().toByteArray()));

        ByteSequence bs = new ByteSequence(ByteBuffer.allocate(Integer.BYTES).putInt(-100).array());
        IOException e = assertThrows(IOException.class, () -> wireFormat.unmarshal(bs));
        assertTrue(e.getMessage().contains("exceeds the maximum frame configured or supported frame size limit"));
    }

    @Test
    public void testFrameSizeTooSmall() throws Exception {
        AmqpHeader inputHeader = new AmqpHeader();
        wireFormat.unmarshal(new ByteSequence(inputHeader.getBuffer().toByteArray()));

        // less than 8
        ByteSequence bs = new ByteSequence(ByteBuffer.allocate(Integer.BYTES).putInt(3).array());
        IOException e = assertThrows(IOException.class, () -> wireFormat.unmarshal(bs));
        assertTrue(e.getMessage().contains("is smaller than the minimally viable frame size value"));
    }

    @Test
    public void testFrameSizeTooLarge() throws Exception {
        wireFormat.setMaxFrameSize(100);
        AmqpHeader inputHeader = new AmqpHeader();
        wireFormat.unmarshal(new ByteSequence(inputHeader.getBuffer().toByteArray()));

        // size 300 is larger than maxFrameSize
        ByteSequence bs = new ByteSequence(ByteBuffer.allocate(Integer.BYTES).putInt(300).array());
        IOException e = assertThrows(IOException.class, () -> wireFormat.unmarshal(bs));
        assertTrue(e.getMessage().contains("larger than max allowed"));
    }

}
