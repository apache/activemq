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
package org.apache.activemq.transport.udp;

import java.io.IOException;
import java.net.SocketAddress;

import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.reliable.ReplayBuffer;
import org.apache.activemq.transport.reliable.Replayer;

/**
 *
 * 
 */
public interface CommandChannel extends Replayer, Service {

    Command read() throws IOException;

    void write(Command command, SocketAddress address) throws IOException;

    int getDatagramSize();

    /**
     * Sets the default size of a datagram on the network.
     */
    void setDatagramSize(int datagramSize);

    DatagramHeaderMarshaller getHeaderMarshaller();

    void setHeaderMarshaller(DatagramHeaderMarshaller headerMarshaller);

    void setTargetAddress(SocketAddress address);

    void setReplayAddress(SocketAddress address);

    void setReplayBuffer(ReplayBuffer replayBuffer);
    
    public int getReceiveCounter();

}
