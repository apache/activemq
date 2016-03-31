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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.activemq.transport.amqp.AmqpWireFormat.ResetListener;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State based Frame reader that is used in the NIO based transports where
 * AMQP frames can come in in partial or overlapping forms.
 */
public class AmqpFrameParser {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpFrameParser.class);

    public interface AMQPFrameSink {
        void onFrame(Object frame);
    }

    private static final byte AMQP_FRAME_SIZE_BYTES = 4;
    private static final byte AMQP_HEADER_BYTES = 8;

    private final AMQPFrameSink frameSink;

    private FrameParser currentParser;
    private AmqpWireFormat wireFormat;

    public AmqpFrameParser(AMQPFrameSink sink) {
        this.frameSink = sink;
    }

    public AmqpFrameParser(final TcpTransport transport) {
        this.frameSink = new AMQPFrameSink() {

            @Override
            public void onFrame(Object frame) {
                transport.doConsume(frame);
            }
        };
    }

    public void parse(ByteBuffer incoming) throws Exception {

        if (incoming == null || !incoming.hasRemaining()) {
            return;
        }

        if (currentParser == null) {
            currentParser = initializeHeaderParser();
        }

        // Parser stack will run until current incoming data has all been consumed.
        currentParser.parse(incoming);
    }

    public void reset() {
        currentParser = initializeHeaderParser();
    }

    private void validateFrameSize(int frameSize) throws IOException {
        long maxFrameSize = AmqpWireFormat.DEFAULT_MAX_FRAME_SIZE;
        if (wireFormat != null) {
            maxFrameSize = wireFormat.getMaxFrameSize();
        }

        if (frameSize > maxFrameSize) {
            throw IOExceptionSupport.createFrameSizeException(frameSize, maxFrameSize);
        }
    }

    public void setWireFormat(AmqpWireFormat wireFormat) {
        this.wireFormat = wireFormat;
        if (wireFormat != null) {
            wireFormat.setProtocolResetListener(new ResetListener() {

                @Override
                public void onProtocolReset() {
                    reset();
                }
            });
        }
    }

    public AmqpWireFormat getWireFormat() {
        return this.wireFormat;
    }

    //----- Prepare the current frame parser for use -------------------------//

    private FrameParser initializeHeaderParser() {
        headerReader.reset(AMQP_HEADER_BYTES);
        return headerReader;
    }

    private FrameParser initializeFrameLengthParser() {
        frameSizeReader.reset(AMQP_FRAME_SIZE_BYTES);
        return frameSizeReader;
    }

    private FrameParser initializeContentReader(int contentLength) {
        contentReader.reset(contentLength);
        return contentReader;
    }

    //----- Frame parser implementations -------------------------------------//

    private interface FrameParser {

        void parse(ByteBuffer incoming) throws IOException;

        void reset(int nextExpectedReadSize);
    }

    private final FrameParser headerReader = new FrameParser() {

        private final Buffer header = new Buffer(AMQP_HEADER_BYTES);

        @Override
        public void parse(ByteBuffer incoming) throws IOException {
            int length = Math.min(incoming.remaining(), header.length - header.offset);

            incoming.get(header.data, header.offset, length);
            header.offset += length;

            if (header.offset == AMQP_HEADER_BYTES) {
                header.reset();
                AmqpHeader amqpHeader = new AmqpHeader(header.deepCopy(), false);
                currentParser = initializeFrameLengthParser();
                frameSink.onFrame(amqpHeader);
                if (incoming.hasRemaining()) {
                    currentParser.parse(incoming);
                }
            }
        }

        @Override
        public void reset(int nextExpectedReadSize) {
            header.reset();
        }
    };

    private final FrameParser frameSizeReader = new FrameParser() {

        private int frameSize;
        private int multiplier;

        @Override
        public void parse(ByteBuffer incoming) throws IOException {

            while (incoming.hasRemaining()) {
                frameSize += ((incoming.get() & 0xFF) << --multiplier * Byte.SIZE);

                if (multiplier == 0) {
                    LOG.trace("Next incoming frame length: {}", frameSize);
                    validateFrameSize(frameSize);
                    currentParser = initializeContentReader(frameSize);
                    if (incoming.hasRemaining()) {
                        currentParser.parse(incoming);
                        return;
                    }
                }
            }
        }

        @Override
        public void reset(int nextExpectedReadSize) {
            multiplier = AMQP_FRAME_SIZE_BYTES;
            frameSize = 0;
        }
    };

    private final FrameParser contentReader = new FrameParser() {

        private Buffer frame;

        @Override
        public void parse(ByteBuffer incoming) throws IOException {
            int length = Math.min(incoming.remaining(), frame.getLength() - frame.offset);
            incoming.get(frame.data, frame.offset, length);
            frame.offset += length;

            if (frame.offset == frame.length) {
                LOG.trace("Contents of size {} have been read", frame.length);
                frame.reset();
                frameSink.onFrame(frame);
                if (currentParser == this) {
                    currentParser = initializeFrameLengthParser();
                }
                if (incoming.hasRemaining()) {
                    currentParser.parse(incoming);
                }
            }
        }

        @Override
        public void reset(int nextExpectedReadSize) {
            // Allocate a new Buffer to hold the incoming frame.  We must write
            // back the frame size value before continue on to read the indicated
            // frame size minus the size of the AMQP frame size header value.
            frame = new Buffer(nextExpectedReadSize);
            frame.bigEndianEditor().writeInt(nextExpectedReadSize);

            // Reset the length to total length as we do direct write after this.
            frame.length = frame.data.length;
        }
    };
}
