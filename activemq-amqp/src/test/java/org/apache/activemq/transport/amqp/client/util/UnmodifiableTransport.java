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
package org.apache.activemq.transport.amqp.client.util;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Record;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Ssl;
import org.apache.qpid.proton.engine.SslDomain;
import org.apache.qpid.proton.engine.SslPeerDetails;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportException;
import org.apache.qpid.proton.engine.TransportResult;

/**
 * Unmodifiable Transport wrapper used to prevent test code from accidentally
 * modifying Transport state.
 */
public class UnmodifiableTransport implements Transport {

    private final Transport transport;

    public UnmodifiableTransport(Transport transport) {
        this.transport = transport;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void free() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public Object getContext() {
        return null;
    }

    @Override
    public EndpointState getLocalState() {
        return transport.getLocalState();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return transport.getRemoteCondition();
    }

    @Override
    public EndpointState getRemoteState() {
        return transport.getRemoteState();
    }

    @Override
    public void open() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void setCondition(ErrorCondition arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void setContext(Object arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void bind(Connection arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public int capacity() {
        return transport.capacity();
    }

    @Override
    public void close_head() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void close_tail() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public int getChannelMax() {
        return transport.getChannelMax();
    }

    @Override
    public ErrorCondition getCondition() {
        return transport.getCondition();
    }

    @Override
    public int getIdleTimeout() {
        return transport.getIdleTimeout();
    }

    @Override
    public ByteBuffer getInputBuffer() {
        return null;
    }

    @Override
    public int getMaxFrameSize() {
        return transport.getMaxFrameSize();
    }

    @Override
    public ByteBuffer getOutputBuffer() {
        return null;
    }

    @Override
    public int getRemoteChannelMax() {
        return transport.getRemoteChannelMax();
    }

    @Override
    public int getRemoteIdleTimeout() {
        return transport.getRemoteIdleTimeout();
    }

    @Override
    public int getRemoteMaxFrameSize() {
        return transport.getRemoteMaxFrameSize();
    }

    @Override
    public ByteBuffer head() {
        return null;
    }

    @Override
    public int input(byte[] arg0, int arg1, int arg2) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public boolean isClosed() {
        return transport.isClosed();
    }

    @Override
    public int output(byte[] arg0, int arg1, int arg2) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void outputConsumed() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public int pending() {
        return transport.pending();
    }

    @Override
    public void pop(int arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void process() throws TransportException {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public TransportResult processInput() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public Sasl sasl() throws IllegalStateException {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void setChannelMax(int arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void setIdleTimeout(int arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void setMaxFrameSize(int arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public Ssl ssl(SslDomain arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public Ssl ssl(SslDomain arg0, SslPeerDetails arg1) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public ByteBuffer tail() {
        return null;
    }

    @Override
    public long tick(long arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void trace(int arg0) {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public void unbind() {
        throw new UnsupportedOperationException("Cannot alter the Transport");
    }

    @Override
    public Record attachments() {
        return transport.attachments();
    }

    @Override
    public long getFramesInput() {
        return transport.getFramesInput();
    }

    @Override
    public long getFramesOutput() {
        return transport.getFramesOutput();
    }
}
