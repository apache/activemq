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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.activemq.transport.TransportSupport;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpNioTransportHelper {

    private final DataInputStream amqpHeaderValue = new DataInputStream(new ByteArrayInputStream(new byte[] { 'A', 'M', 'Q', 'P' }));
    private final Integer AMQP_HEADER_VALUE;
    private static final Logger LOG = LoggerFactory.getLogger(AmqpNioTransportHelper.class);
    protected int nextFrameSize = -1;
    protected ByteBuffer currentBuffer;
    private boolean magicConsumed = false;
    private final TransportSupport transportSupport;

    public AmqpNioTransportHelper(TransportSupport transportSupport) throws IOException {
        AMQP_HEADER_VALUE = amqpHeaderValue.readInt();
        this.transportSupport = transportSupport;
    }

    protected void processCommand(ByteBuffer plain) throws Exception {
        // Are we waiting for the next Command or building on the current one?
        // The frame size is in the first 4 bytes.
        if (nextFrameSize == -1) {
            // We can get small packets that don't give us enough for the frame
            // size so allocate enough for the initial size value and
            if (plain.remaining() < 4) {
                if (currentBuffer == null) {
                    currentBuffer = ByteBuffer.allocate(4);
                }

                // Go until we fill the integer sized current buffer.
                while (currentBuffer.hasRemaining() && plain.hasRemaining()) {
                    currentBuffer.put(plain.get());
                }

                // Didn't we get enough yet to figure out next frame size.
                if (currentBuffer.hasRemaining()) {
                    return;
                } else {
                    currentBuffer.flip();
                    nextFrameSize = currentBuffer.getInt();
                }
            } else {
                // Either we are completing a previous read of the next frame
                // size or its fully contained in plain already.
                if (currentBuffer != null) {
                    // Finish the frame size integer read and get from the
                    // current buffer.
                    while (currentBuffer.hasRemaining()) {
                        currentBuffer.put(plain.get());
                    }

                    currentBuffer.flip();
                    nextFrameSize = currentBuffer.getInt();
                } else {
                    nextFrameSize = plain.getInt();
                }
            }
        }

        // There are three possibilities when we get here. We could have a
        // partial frame, a full frame, or more than 1 frame
        while (true) {
            // handle headers, which start with 'A','M','Q','P' rather than size
            if (nextFrameSize == AMQP_HEADER_VALUE) {
                nextFrameSize = handleAmqpHeader(plain);
                if (nextFrameSize == -1) {
                    return;
                }
            }
            validateFrameSize(nextFrameSize);

            // now we have the data, let's reallocate and try to fill it,
            // (currentBuffer.putInt() is called TODO update
            // because we need to put back the 4 bytes we read to determine the
            // size)
            if (currentBuffer == null || (currentBuffer.limit() == 4)) {
                currentBuffer = ByteBuffer.allocate(nextFrameSize);
                currentBuffer.putInt(nextFrameSize);
            }

            if (currentBuffer.remaining() >= plain.remaining()) {
                currentBuffer.put(plain);
            } else {
                byte[] fill = new byte[currentBuffer.remaining()];
                plain.get(fill);
                currentBuffer.put(fill);
            }

            // Either we have enough data for a new command or we have to wait for some more.
            // If hasRemaining is true, we have not filled the buffer yet, i.e. we haven't
            // received the full frame.
            if (currentBuffer.hasRemaining()) {
                return;
            } else {
                currentBuffer.flip();
                LOG.debug("Calling doConsume with position {} limit {}", currentBuffer.position(), currentBuffer.limit());
                transportSupport.doConsume(AmqpSupport.toBuffer(currentBuffer));
                currentBuffer = null;
                nextFrameSize = -1;

                // Determine if there are more frames to process
                if (plain.hasRemaining()) {
                    if (plain.remaining() < 4) {
                        currentBuffer = ByteBuffer.allocate(4);
                        while (currentBuffer.hasRemaining() && plain.hasRemaining()) {
                            currentBuffer.put(plain.get());
                        }
                        return;
                    } else {
                        nextFrameSize = plain.getInt();
                    }
                } else {
                    return;
                }
            }
        }
    }

    private void validateFrameSize(int frameSize) throws IOException {
        if (nextFrameSize > AmqpWireFormat.DEFAULT_MAX_FRAME_SIZE) {
            throw new IOException("Frame size of " + nextFrameSize + "larger than max allowed " + AmqpWireFormat.DEFAULT_MAX_FRAME_SIZE);
        }
    }

    private int handleAmqpHeader(ByteBuffer plain) {
        int nextFrameSize;

        LOG.debug("Consuming AMQP_HEADER");
        currentBuffer = ByteBuffer.allocate(8);
        currentBuffer.putInt(AMQP_HEADER_VALUE);
        while (currentBuffer.hasRemaining()) {
            currentBuffer.put(plain.get());
        }
        currentBuffer.flip();
        if (!magicConsumed) { // The first case we see is special and has to be handled differently
            transportSupport.doConsume(new AmqpHeader(new Buffer(currentBuffer)));
            magicConsumed = true;
        } else {
            transportSupport.doConsume(AmqpSupport.toBuffer(currentBuffer));
        }
        currentBuffer = null;

        if (plain.hasRemaining()) {
            if (plain.remaining() < 4) {
                nextFrameSize = 4;
            } else {
                nextFrameSize = plain.getInt();
            }
        } else {
            nextFrameSize = -1;
        }

        return nextFrameSize;
    }
}
