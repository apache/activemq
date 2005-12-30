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
package org.apache.activeio.stream.sync.socket;

import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.SocketException;

/**
 */
public interface SocketMetadata {
    public InetAddress getInetAddress();

    public boolean getKeepAlive() throws SocketException;

    public InetAddress getLocalAddress();

    public int getLocalPort();

    public SocketAddress getLocalSocketAddress();

    public int getPort();

    public int getReceiveBufferSize() throws SocketException;

    public SocketAddress getRemoteSocketAddress();

    public boolean getReuseAddress() throws SocketException;

    public int getSendBufferSize() throws SocketException;

    public boolean getOOBInline() throws SocketException;

    public int getSoLinger() throws SocketException;

    public int getSoTimeout() throws SocketException;

    public boolean getTcpNoDelay() throws SocketException;

    public int getTrafficClass() throws SocketException;

    public boolean isBound();

    public boolean isClosed();

    public boolean isConnected();

    public void setKeepAlive(boolean on) throws SocketException;

    public void setOOBInline(boolean on) throws SocketException;

    public void setReceiveBufferSize(int size) throws SocketException;

    public void setReuseAddress(boolean on) throws SocketException;

    public void setSendBufferSize(int size) throws SocketException;

    public void setSoLinger(boolean on, int linger) throws SocketException;
    
    public void setSoTimeout(int i) throws SocketException;

    public void setTcpNoDelay(boolean on) throws SocketException;

    public void setTrafficClass(int tc) throws SocketException;
}