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
package org.apache.activemq.transport.amqp;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;

import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.amqp.AmqpFrameParser.AMQPFrameSink;
import org.apache.activemq.transport.ws.WSTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;

/**
 * An AMQP based WebSocket transport implementation.
 */
public class AmqpWSTransport extends TransportSupport implements WSTransport, AMQPFrameSink {

    private final AmqpFrameParser frameReader = new AmqpFrameParser(this);
    private final URI remoteLocation;

    private WSTransportSink outputSink;
    private int receiveCounter;
    private X509Certificate[] certificates;

    /**
     * Create a new Transport instance.
     *
     * @param location
     *      the remote location where the client connection is from.
     * @param wireFormat
     *      the WireFormat instance that configures this Transport.
     */
    public AmqpWSTransport(URI location, WireFormat wireFormat) {
        super();

        remoteLocation = location;
        frameReader.setWireFormat((AmqpWireFormat) wireFormat);
    }

    @Override
    public void setTransportSink(WSTransportSink outputSink) {
        this.outputSink = outputSink;
    }

    @Override
    public void oneway(Object command) throws IOException {
        if (command instanceof ByteBuffer) {
            outputSink.onSocketOutboundBinary((ByteBuffer) command);
        } else {
            throw new IOException("Unexpected output command.");
        }
    }

    @Override
    public String getRemoteAddress() {
        return remoteLocation.toASCIIString();
    }

    @Override
    public int getReceiveCounter() {
        return receiveCounter;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return certificates;
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
        this.certificates = certificates;
    }

    @Override
    public String getSubProtocol() {
        return "amqp";
    }

    @Override
    public WireFormat getWireFormat() {
        return frameReader.getWireFormat();
    }

    @Override
    public int getMaxFrameSize() {
        return (int) Math.min(((AmqpWireFormat) getWireFormat()).getMaxFrameSize(), Integer.MAX_VALUE);
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        // Currently nothing needed here since we have no async workers.
    }

    @Override
    protected void doStart() throws Exception {
        if (outputSink == null) {
            throw new IllegalStateException("Transport started before output sink assigned.");
        }

        // Currently nothing needed here since we have no async workers.
    }

    //----- WebSocket event hooks --------------------------------------------//

    @Override
    public void onWebSocketText(String data) throws IOException {
        onException(new IOException("Illegal text content receive on AMQP WebSocket channel."));
    }

    @Override
    public void onWebSocketBinary(ByteBuffer data) throws IOException {
        try {
            frameReader.parse(data);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void onWebSocketClosed() throws IOException {
        onException(new IOException("Unexpected close of AMQP WebSocket channel."));
    }

    //----- AMQP Frame Data event hook ---------------------------------------//

    @Override
    public void onFrame(Object frame) {
        doConsume(frame);
    }
}
