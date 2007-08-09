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
 * @version $Revision$
 */
public interface CommandChannel extends Replayer, Service {

    public Command read() throws IOException;

    public void write(Command command, SocketAddress address) throws IOException;

    public int getDatagramSize();

    /**
     * Sets the default size of a datagram on the network.
     */
    public void setDatagramSize(int datagramSize);

    public DatagramHeaderMarshaller getHeaderMarshaller();

    public void setHeaderMarshaller(DatagramHeaderMarshaller headerMarshaller);

    public void setTargetAddress(SocketAddress address);

    public void setReplayAddress(SocketAddress address);

    public void setReplayBuffer(ReplayBuffer replayBuffer);
}
