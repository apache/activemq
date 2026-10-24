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
package org.apache.activemq.transport.tcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TcpTransportSocketBufferSizeTest {

    private final List<LogEvent> transportEvents = new CopyOnWriteArrayList<>();
    private Logger transportLogger;
    private Appender appender;
    private Socket socket;

    @Before
    public void setUp() {
        appender = new AbstractAppender("socketBufferSizeAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                    transportEvents.add(event.toImmutable());
                }
            }
        };
        appender.start();
        transportLogger = (Logger) LogManager.getLogger(TcpTransport.class);
        transportLogger.addAppender(appender);
        socket = new Socket();
    }

    @After
    public void tearDown() throws Exception {
        transportLogger.removeAppender(appender);
        appender.stop();
        socket.close();
    }

    @Test
    public void testZeroKeepsOsDefaultsWithoutLogging() throws Exception {
        // Arrange
        int defaultReceive = socket.getReceiveBufferSize();
        int defaultSend = socket.getSendBufferSize();
        TcpTransport transport = newTransport(0);

        // Act
        transport.initialiseSocket(socket);

        // Assert
        assertEquals(defaultReceive, socket.getReceiveBufferSize());
        assertEquals(defaultSend, socket.getSendBufferSize());
        assertTrue("no warn/error expected: " + transportEvents, transportEvents.isEmpty());
    }

    @Test
    public void testNegativeLogsWarningAndKeepsOsDefaults() throws Exception {
        // Arrange
        int defaultReceive = socket.getReceiveBufferSize();
        int defaultSend = socket.getSendBufferSize();
        TcpTransport transport = newTransport(-1);

        // Act
        transport.initialiseSocket(socket);

        // Assert
        assertEquals(defaultReceive, socket.getReceiveBufferSize());
        assertEquals(defaultSend, socket.getSendBufferSize());
        assertEquals(1, transportEvents.size());
        assertEquals(Level.WARN, transportEvents.get(0).getLevel());
        assertTrue(transportEvents.get(0).getMessage().getFormattedMessage().contains("-1"));
    }

    @Test
    public void testPositiveSetsBuffersWithoutLogging() throws Exception {
        // Arrange
        TcpTransport transport = newTransport(128 * 1024);

        // Act
        transport.initialiseSocket(socket);

        // Assert
        assertTrue(socket.getReceiveBufferSize() >= 128 * 1024);
        assertTrue(socket.getSendBufferSize() >= 128 * 1024);
        assertTrue("no warn/error expected: " + transportEvents, transportEvents.isEmpty());
    }

    private TcpTransport newTransport(int socketBufferSize) throws Exception {
        TcpTransport transport = new TcpTransport(new OpenWireFormat(), new Socket());
        transport.setSocketBufferSize(socketBufferSize);
        return transport;
    }
}
