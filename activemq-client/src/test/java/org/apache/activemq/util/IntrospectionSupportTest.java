/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Test;

public class IntrospectionSupportTest {

    private class DummyClass {

        private boolean trace;

        DummyClass(boolean trace) {
            this.trace = trace;
        }

        public boolean isTrace() {
            return trace;
        }

        public void setTrace(boolean trace) {
            this.trace = trace;
        }
    }

    @Test
    public void testSetPropertyPrimitiveWithWrapperValue() {
        // Wrapper value
        Boolean value = Boolean.TRUE;

        DummyClass dummyClass = new DummyClass(false);
        dummyClass.setTrace(false);

        // dummy field expects a primitive
        IntrospectionSupport.setProperty(dummyClass, "trace", value);

        assertTrue(dummyClass.isTrace());
    }

    // nio channels return JDK internal socket classes that reflection cannot access,
    // so the setter has to be resolved against the public java.net API instead
    @Test
    public void testSetPropertyOnNioSocket() throws Exception {
        try (var channel = SocketChannel.open()) {
            var socket = channel.socket();
            assertTrue(IntrospectionSupport.setProperty(socket, "tcpNoDelay", "true"));
            assertTrue(socket.getTcpNoDelay());
            assertTrue(IntrospectionSupport.setProperty(socket, "soTimeout", "1234"));
            assertEquals(1234, socket.getSoTimeout());
        }
    }

    @Test
    public void testSetPropertyOnNioServerSocket() throws Exception {
        try (var channel = ServerSocketChannel.open()) {
            var socket = channel.socket();
            assertTrue(IntrospectionSupport.setProperty(socket, "soTimeout", "1234"));
            assertEquals(1234, socket.getSoTimeout());
        }
    }
}
