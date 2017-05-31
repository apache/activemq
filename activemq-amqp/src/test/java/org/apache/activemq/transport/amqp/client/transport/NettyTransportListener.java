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
package org.apache.activemq.transport.amqp.client.transport;

import io.netty.buffer.ByteBuf;

/**
 * Listener interface that should be implemented by users of the various
 * QpidJMS Transport classes.
 */
public interface NettyTransportListener {

    /**
     * Called when new incoming data has become available.
     *
     * @param incoming
     *        the next incoming packet of data.
     */
    void onData(ByteBuf incoming);

    /**
     * Called if the connection state becomes closed.
     */
    void onTransportClosed();

    /**
     * Called when an error occurs during normal Transport operations.
     *
     * @param cause
     *        the error that triggered this event.
     */
    void onTransportError(Throwable cause);

}
