/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.udp;

import org.apache.activemq.Service;
import org.apache.activemq.command.Command;

import java.io.IOException;
import java.net.SocketAddress;

/**
 *
 * @version $Revision$
 */
public interface CommandChannel extends Service {

    public abstract Command read() throws IOException;

    public abstract void write(Command command) throws IOException;

    public abstract void write(Command command, SocketAddress address) throws IOException;

    public abstract int getDatagramSize();

    /**
     * Sets the default size of a datagram on the network.
     */
    public abstract void setDatagramSize(int datagramSize);

    public abstract DatagramHeaderMarshaller getHeaderMarshaller();

    public abstract void setHeaderMarshaller(DatagramHeaderMarshaller headerMarshaller);

}