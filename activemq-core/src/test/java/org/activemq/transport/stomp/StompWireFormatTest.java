/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.broker.BrokerService;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.Response;
import org.activemq.command.SessionInfo;

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

        wire.registerTransportStreams(dout, din);
        wire.initiateServerSideProtocol();

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
