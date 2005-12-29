/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompWireFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

public class StompWireFormatTest extends TestCase {
    private StompWireFormat wire;

    public void setUp() throws Exception {
        wire = new StompWireFormat();
    }

    public void testDummy() throws Exception {
    }
    
    public void TODO_testValidConnectHandshake() throws Exception {
        String connect_frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n" + "\n" + Stomp.NULL;
        DataInputStream din = new DataInputStream(new ByteArrayInputStream(connect_frame.getBytes()));
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        ConnectionInfo ci = (ConnectionInfo) wire.readCommand(din);
        assertNotNull(ci);
        assertTrue(ci.isResponseRequired());

        Response cr = new Response();
        cr.setCorrelationId(ci.getCommandId());
        wire.writeCommand(cr, dout);

        SessionInfo si = (SessionInfo) wire.readCommand(null);
        assertNotNull(si);
        assertTrue(si.isResponseRequired());

        Response sr = new Response();
        sr.setCorrelationId(si.getCommandId());
        wire.writeCommand(sr, dout);

        String response = new String(bout.toByteArray());
        assertTrue(response.startsWith("CONNECTED"));
    }

    public void _testFakeServer() throws Exception {
        final BrokerService container = new BrokerService();
        new Thread(new Runnable() {
            public void run() {
                try {
                    container.addConnector("stomp://localhost:61613");
                    container.start();
                }
                catch (Exception e) {
                    System.err.println("ARGH: caught: " + e);
                    e.printStackTrace();
                }
            }
        }).start();
        System.err.println("started container");
        System.err.println("okay, go play");

        System.err.println(System.in.read());
    }
}
