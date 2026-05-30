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

package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class StompWireFormatFactoryTest {

    @Test
    public void testDefaults() {
        StompWireFormatFactory factory = new StompWireFormatFactory();

        StompWireFormat wireFormat = factory.createWireFormat();
        assertEquals(StompWireFormat.DEFAULT_MAX_FRAME_SIZE, wireFormat.getMaxFrameSize());
        assertEquals(StompWireFormat.MAX_DATA_LENGTH, wireFormat.getMaxDataLength());
        assertEquals(StompWireFormat.DEFAULT_SERVER_MODE, wireFormat.isServerMode());
    }

    @Test
    public void testSetters() {
        StompWireFormatFactory factory = new StompWireFormatFactory();
        factory.setMaxFrameSize(1020L);
        factory.setMaxDataLength(2040);
        factory.setServerMode(false);

        StompWireFormat wireFormat = factory.createWireFormat();
        assertEquals(1020L, wireFormat.getMaxFrameSize());
        assertEquals(2040, wireFormat.getMaxDataLength());
        assertFalse(wireFormat.isServerMode());
    }
}
