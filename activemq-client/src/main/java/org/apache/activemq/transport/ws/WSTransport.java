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
package org.apache.activemq.transport.ws;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.activemq.transport.Transport;

/**
 * Interface for a WebSocket Transport which provide hooks that a servlet can
 * use to pass along WebSocket data and events.
 */
public interface WSTransport extends Transport {

    /**
     * WS Transport output sink, used to give the WS Transport implementation
     * a way to produce output back to the WS connection without coupling it
     * to the implementation.
     */
    public interface WSTransportSink {

        /**
         * Called from the Transport when new outgoing String data is ready.
         *
         * @param data
         *      The newly prepared outgoing string data.
         *
         * @throws IOException if an error occurs or the socket doesn't support text data.
         */
        void onSocketOutboundText(String data) throws IOException;

        /**
         * Called from the Transport when new outgoing String data is ready.
         *
         * @param data
         *      The newly prepared outgoing string data.
         *
         * @throws IOException if an error occurs or the socket doesn't support text data.
         */
        void onSocketOutboundBinary(ByteBuffer data) throws IOException;
    }

    /**
     * @return the maximum frame size allowed for this WS Transport.
     */
    int getMaxFrameSize();

    /**
     * @return the WS sub-protocol that this transport is supplying.
     */
    String getSubProtocol();

    /**
     * Called to provide the WS with the output data sink.
     */
    void setTransportSink(WSTransportSink outputSink);

    /**
     * Called from the WebSocket framework when new incoming String data is received.
     *
     * @param data
     *      The newly received incoming data.
     *
     * @throws IOException if an error occurs or the socket doesn't support text data.
     */
    void onWebSocketText(String data) throws IOException;

    /**
     * Called from the WebSocket framework when new incoming Binary data is received.
     *
     * @param data
     *      The newly received incoming data.
     *
     * @throws IOException if an error occurs or the socket doesn't support binary data.
     */
    void onWebSocketBinary(ByteBuffer data) throws IOException;

    /**
     * Called from the WebSocket framework when the socket has been closed unexpectedly.
     *
     * @throws IOException if an error while processing the close.
     */
    void onWebSocketClosed() throws IOException;

}
