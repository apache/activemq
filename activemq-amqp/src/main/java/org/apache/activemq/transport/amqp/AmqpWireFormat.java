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
package org.apache.activemq.transport.amqp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.activemq.transport.amqp.message.InboundTransformer;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpWireFormat implements WireFormat {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpWireFormat.class);

    public static final long DEFAULT_MAX_FRAME_SIZE = Long.MAX_VALUE;
    public static final int NO_AMQP_MAX_FRAME_SIZE = -1;
    public static final int DEFAULT_CONNECTION_TIMEOUT = 30000;
    public static final int DEFAULT_IDLE_TIMEOUT = 30000;
    public static final int DEFAULT_PRODUCER_CREDIT = 1000;
    public static final boolean DEFAULT_ALLOW_NON_SASL_CONNECTIONS = false;
    public static final int DEFAULT_ANQP_FRAME_SIZE = 128 * 1024;

    private static final int SASL_PROTOCOL = 3;

    private int version = 1;
    private long maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private int maxAmqpFrameSize = DEFAULT_ANQP_FRAME_SIZE;
    private int connectAttemptTimeout = DEFAULT_CONNECTION_TIMEOUT;
    private int idelTimeout = DEFAULT_IDLE_TIMEOUT;
    private int producerCredit = DEFAULT_PRODUCER_CREDIT;
    private String transformer = InboundTransformer.TRANSFORMER_JMS;
    private boolean allowNonSaslConnections = DEFAULT_ALLOW_NON_SASL_CONNECTIONS;

    private boolean magicRead = false;
    private ResetListener resetListener;

    public interface ResetListener {
        void onProtocolReset();
    }

    @Override
    public ByteSequence marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        marshal(command, dos);
        dos.close();
        return baos.toByteSequence();
    }

    @Override
    public Object unmarshal(ByteSequence packet) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(packet);
        DataInputStream dis = new DataInputStream(stream);
        return unmarshal(dis);
    }

    @Override
    public void marshal(Object command, DataOutput dataOut) throws IOException {
        if (command instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) command;

            if (dataOut instanceof OutputStream) {
                WritableByteChannel channel = Channels.newChannel((OutputStream) dataOut);
                channel.write(buffer);
            } else {
                while (buffer.hasRemaining()) {
                    dataOut.writeByte(buffer.get());
                }
            }
        } else {
            Buffer frame = (Buffer) command;
            frame.writeTo(dataOut);
        }
    }

    @Override
    public Object unmarshal(DataInput dataIn) throws IOException {
        if (!magicRead) {
            Buffer magic = new Buffer(8);
            magic.readFrom(dataIn);
            magicRead = true;
            return new AmqpHeader(magic, false);
        } else {
            int size = dataIn.readInt();
            if (size > maxFrameSize) {
                throw new AmqpProtocolException("Frame size exceeded max frame length.");
            } else if (size <= 0) {
                throw new AmqpProtocolException("Frame size value was invalid: " + size);
            }
            Buffer frame = new Buffer(size);
            frame.bigEndianEditor().writeInt(size);
            frame.readFrom(dataIn);
            frame.clear();
            return frame;
        }
    }

    /**
     * Given an AMQP header validate that the AMQP magic is present and
     * if so that the version and protocol values align with what we support.
     *
     * In the case where authentication occurs the client sends us two AMQP
     * headers, the first being the SASL initial header which triggers the
     * authentication process and then if that succeeds we should get a second
     * AMQP header that does not contain the SASL protocol ID indicating the
     * connection process should follow the normal path.  We validate that the
     * header align with these expectations.
     *
     * @param header
     *        the header instance received from the client.
     * @param authenticated
     *        has the client already authenticated already.
     *
     * @return true if the header is valid against the current WireFormat.
     */
    public boolean isHeaderValid(AmqpHeader header, boolean authenticated) {
        if (!header.hasValidPrefix()) {
            LOG.trace("AMQP Header arrived with invalid prefix: {}", header);
            return false;
        }

        if (!(header.getProtocolId() == 0 || header.getProtocolId() == SASL_PROTOCOL)) {
            LOG.trace("AMQP Header arrived with invalid protocol ID: {}", header);
            return false;
        }

        if (!authenticated && !isAllowNonSaslConnections() && header.getProtocolId() != SASL_PROTOCOL) {
            LOG.trace("AMQP Header arrived without SASL and server requires SASL: {}", header);
            return false;
        }

        if (header.getMajor() != 1 || header.getMinor() != 0 || header.getRevision() != 0) {
            LOG.trace("AMQP Header arrived invalid version: {}", header);
            return false;
        }

        return true;
    }

    /**
     * Returns an AMQP Header object that represents the minimally protocol
     * versions supported by this transport.  A client that attempts to
     * connect with an AMQP version that doesn't at least meat this value
     * will receive this prior to the connection being closed.
     *
     * @return the minimal AMQP version needed from the client.
     */
    public AmqpHeader getMinimallySupportedHeader() {
        AmqpHeader header = new AmqpHeader();
        if (!isAllowNonSaslConnections()) {
            header.setProtocolId(3);
        }

        return header;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public int getVersion() {
        return this.version;
    }

    public void resetMagicRead() {
        this.magicRead = false;
        if (resetListener != null) {
            resetListener.onProtocolReset();
        }
    }

    public void setProtocolResetListener(ResetListener listener) {
        this.resetListener = listener;
    }

    public boolean isMagicRead() {
        return this.magicRead;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public int getMaxAmqpFrameSize() {
        return maxAmqpFrameSize;
    }

    public void setMaxAmqpFrameSize(int maxAmqpFrameSize) {
        this.maxAmqpFrameSize = maxAmqpFrameSize;
    }

    public boolean isAllowNonSaslConnections() {
        return allowNonSaslConnections;
    }

    public void setAllowNonSaslConnections(boolean allowNonSaslConnections) {
        this.allowNonSaslConnections = allowNonSaslConnections;
    }

    public int getConnectAttemptTimeout() {
        return connectAttemptTimeout;
    }

    public void setConnectAttemptTimeout(int connectAttemptTimeout) {
        this.connectAttemptTimeout = connectAttemptTimeout;
    }

    public void setProducerCredit(int producerCredit) {
        this.producerCredit = producerCredit;
    }

    public int getProducerCredit() {
        return producerCredit;
    }

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }

    public int getIdleTimeout() {
        return idelTimeout;
    }

    public void setIdleTimeout(int idelTimeout) {
        this.idelTimeout = idelTimeout;
    }
}
