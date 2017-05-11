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

import java.io.IOException;
import java.net.URI;
import java.security.Principal;

import io.netty.buffer.ByteBuf;

/**
 * Base for all Netty based Transports in this client.
 */
public interface NettyTransport {

    void connect() throws IOException;

    boolean isConnected();

    boolean isSSL();

    void close() throws IOException;

    ByteBuf allocateSendBuffer(int size) throws IOException;

    void send(ByteBuf output) throws IOException;

    NettyTransportListener getTransportListener();

    void setTransportListener(NettyTransportListener listener);

    NettyTransportOptions getTransportOptions();

    URI getRemoteLocation();

    Principal getLocalPrincipal();

    void setMaxFrameSize(int maxFrameSize);

    int getMaxFrameSize();

}