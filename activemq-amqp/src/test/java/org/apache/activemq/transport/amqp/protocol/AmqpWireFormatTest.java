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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.transport.amqp.AmqpHeader;
import org.apache.activemq.transport.amqp.AmqpWireFormat;
import org.apache.activemq.transport.amqp.AmqpWireFormat.ResetListener;
import org.junit.Test;

public class AmqpWireFormatTest {

    private final AmqpWireFormat wireFormat = new AmqpWireFormat();

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
}
