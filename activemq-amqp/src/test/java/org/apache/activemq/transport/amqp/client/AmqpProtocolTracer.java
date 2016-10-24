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
package org.apache.activemq.transport.amqp.client;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;
import org.apache.qpid.proton.amqp.transport.Close;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.apache.qpid.proton.amqp.transport.Disposition;
import org.apache.qpid.proton.amqp.transport.End;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.apache.qpid.proton.amqp.transport.FrameBody.FrameBodyHandler;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.framing.TransportFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracer used to spy on AMQP traffic
 */
public class AmqpProtocolTracer implements ProtocolTracer, FrameBodyHandler<AmqpFrameValidator> {

    private static final Logger TRACE_FRAMES = LoggerFactory.getLogger(AmqpProtocolTracer.class.getPackage().getName() + ".FRAMES");

    private final AmqpConnection connection;

    public AmqpProtocolTracer(AmqpConnection connection) {
        this.connection = connection;
    }

    @Override
    public void receivedFrame(TransportFrame transportFrame) {
        if (connection.isTraceFrames()) {
            TRACE_FRAMES.trace("{} | RECV: {}", connection.getRemoteURI(), transportFrame.getBody());
        }

        AmqpFrameValidator inspector = connection.getReceivedFrameInspector();
        if (inspector != null) {
            transportFrame.getBody().invoke(this, transportFrame.getPayload(), inspector);
        }
    }

    @Override
    public void sentFrame(TransportFrame transportFrame) {
        if (connection.isTraceFrames()) {
            TRACE_FRAMES.trace("{} | SENT: {}", connection.getRemoteURI(), transportFrame.getBody());
        }

        AmqpFrameValidator inspector = connection.getSentFrameInspector();
        if (inspector != null) {
            transportFrame.getBody().invoke(this, transportFrame.getPayload(), inspector);
        }
    }

    @Override
    public void handleOpen(Open open, Binary payload, AmqpFrameValidator context) {
        context.inspectOpen(open, payload);
    }

    @Override
    public void handleBegin(Begin begin, Binary payload, AmqpFrameValidator context) {
        context.inspectBegin(begin, payload);
    }

    @Override
    public void handleAttach(Attach attach, Binary payload, AmqpFrameValidator context) {
        context.inspectAttach(attach, payload);
    }

    @Override
    public void handleFlow(Flow flow, Binary payload, AmqpFrameValidator context) {
        context.inspectFlow(flow, payload);
    }

    @Override
    public void handleTransfer(Transfer transfer, Binary payload, AmqpFrameValidator context) {
        context.inspectTransfer(transfer, payload);
    }

    @Override
    public void handleDisposition(Disposition disposition, Binary payload, AmqpFrameValidator context) {
        context.inspectDisposition(disposition, payload);
    }

    @Override
    public void handleDetach(Detach detach, Binary payload, AmqpFrameValidator context) {
        context.inspectDetach(detach, payload);
    }

    @Override
    public void handleEnd(End end, Binary payload, AmqpFrameValidator context) {
        context.inspectEnd(end, payload);
    }

    @Override
    public void handleClose(Close close, Binary payload, AmqpFrameValidator context) {
        context.inspectClose(close, payload);
    }
}
