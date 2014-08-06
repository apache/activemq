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
package org.apache.activemq.proxy;

import org.apache.activemq.Service;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

class ProxyConnection implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyConnection.class);

    protected final Transport localTransport;
    protected final Transport remoteTransport;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ProxyConnection(Transport localTransport, Transport remoteTransport) {
        this.localTransport = localTransport;
        this.remoteTransport = remoteTransport;
    }

    public void onFailure(IOException e) {
        if (!shuttingDown.get()) {
            LOG.debug("Transport error: {}", e.getMessage(), e);
            try {
                stop();
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void start() throws Exception {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        this.localTransport.setTransportListener(new DefaultTransportListener() {
            @Override
            public void onCommand(Object command) {
                boolean shutdown = false;
                if (command.getClass() == ShutdownInfo.class) {
                    shuttingDown.set(true);
                    shutdown = true;
                }
                // skipping WireFormat infos
                if (command.getClass() == WireFormatInfo.class) {
                    return;
                }
                try {
                    remoteTransport.oneway(command);
                    if (shutdown) {
                        stop();
                    }
                } catch (IOException error) {
                    onFailure(error);
                } catch (Exception error) {
                    onFailure(IOExceptionSupport.create(error));
                }
            }

            @Override
            public void onException(IOException error) {
                onFailure(error);
            }
        });

        this.remoteTransport.setTransportListener(new DefaultTransportListener() {
            @Override
            public void onCommand(Object command) {
                try {
                    // skipping WireFormat infos
                    if (command.getClass() == WireFormatInfo.class) {
                        return;
                    }
                    localTransport.oneway(command);
                } catch (IOException error) {
                    onFailure(error);
                }
            }

            @Override
            public void onException(IOException error) {
                onFailure(error);
            }
        });

        localTransport.start();
        remoteTransport.start();
    }

    @Override
    public void stop() throws Exception {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        shuttingDown.set(true);
        ServiceStopper ss = new ServiceStopper();
        ss.stop(remoteTransport);
        ss.stop(localTransport);
        ss.throwFirstException();
    }


    @Override
    public boolean equals(Object arg) {
        if (arg == null || !(arg instanceof ProxyConnection)) {
            return false;
        } else {
            ProxyConnection other = (ProxyConnection) arg;
            String otherRemote = "";
            String otherLocal = "";
            String thisRemote = "";
            String thisLocal = "";

            if (other.localTransport != null && other.localTransport.getRemoteAddress() != null) {
                otherLocal = other.localTransport.getRemoteAddress();
            }
            if (other.remoteTransport != null && other.remoteTransport.getRemoteAddress() != null) {
                otherRemote = other.remoteTransport.getRemoteAddress();
            }
            if (this.remoteTransport != null && this.remoteTransport.getRemoteAddress() != null) {
                thisRemote = this.remoteTransport.getRemoteAddress();
            }
            if (this.localTransport != null && this.localTransport.getRemoteAddress() != null) {
                thisLocal = this.localTransport.getRemoteAddress();
            }

            if (otherRemote.equals(thisRemote) && otherLocal.equals(thisLocal)) {
                return true;
            } else {
                return false;
            }
        }
    }


    @Override
    public int hashCode() {
        int hash = 17;
        if (localTransport != null && localTransport.getRemoteAddress() != null) {
            hash += 31 * hash + localTransport.getRemoteAddress().hashCode();
        }
        if (remoteTransport != null && remoteTransport.getRemoteAddress() != null) {
            hash = 31 * hash + remoteTransport.hashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
     	return "ProxyConnection [localTransport=" + localTransport
                + ", remoteTransport=" + remoteTransport + ", shuttingDown="
                + shuttingDown.get() + ", running=" + running.get() + "]";
    }
}
